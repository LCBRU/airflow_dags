import logging
import os
import pathlib
import subprocess
from datetime import UTC, date, datetime, timedelta

from airflow import DAG
from airflow.sdk import task
from dateutil.relativedelta import relativedelta

from tools import default_dag_args, email_notification_callback
from warehousing.database import (
    LIVE_DB_CONNECTION_NAME,
    OPS_DB_CONNECTION_NAME,
    MySqlConnection,
)
from warehousing.schedules import SCHEDULE_BACKUP

logger = logging.getLogger(__name__)

BACKUP_DIRECTORY = '/backup/live_db/'


@task(on_failure_callback=email_notification_callback)
def _backup_database(conn, db):
    logger.info("_backup_database: Started")

    dump = subprocess.Popen(
        [
            'mysqldump',
            '-h',
            conn.host,
            '-u',
            conn.login,
            f'--password={conn.password}',
            '--add-drop-database',
            '--databases',
            db,
        ],
        stdout=subprocess.PIPE,
        text=True,
    )

    backup_dir = pathlib.Path(BACKUP_DIRECTORY) / conn.host / f"{datetime.now():%Y%m%d}"
    backup_dir.mkdir(parents=True, exist_ok=True)

    with open(pathlib.Path(backup_dir / f"{db}_{datetime.now():%Y%m%d_%H%M%S}.sql.gz"), "w") as zipfile:
        zip = subprocess.Popen(
            [
                'gzip',
                '-c',
            ],
            stdin=dump.stdout,
            stdout=zipfile,
        )

    _, errors = zip.communicate()
    logger.error(errors)
    logger.info("_backup_database: Ended")


@task(on_failure_callback=email_notification_callback)
def _cleanup_old_backups():
    logger.info("_cleanup_old_backups: Started")

    backup_dir = pathlib.Path(BACKUP_DIRECTORY)
    backup_dir.mkdir(parents=True, exist_ok=True)

    today = date.today()
    oldest_daily = today - timedelta(days=7)
    oldest_weekly = today - timedelta(weeks=4)
    oldest_monthly = today - relativedelta(months=12)
    oldest_yearly = today - relativedelta(years=5)

    logger.info(f"Oldest daily date: {oldest_daily!r}")
    logger.info(f"Oldest weekly date: {oldest_weekly!r}")
    logger.info(f"Oldest monthly date: {oldest_monthly!r}")
    logger.info(f"Oldest yearly date: {oldest_yearly!r}")

    to_delete = []

    for f in [f for f in backup_dir.glob('**/*') if f.is_file()]:
        modifield_date = datetime.fromtimestamp(f.stat().st_mtime, tz=UTC).date()

        if modifield_date >= oldest_daily:
            logger.info(f"Keeping file {f} because it's younger than daily oldest daily date")
            continue
    
        if modifield_date >= oldest_weekly and modifield_date.weekday() == 0:
            logger.info(f"Keeping file {f} because it's younger than daily oldest weekly date")
            continue

        if modifield_date >= oldest_monthly and modifield_date.day == 1:
            logger.info(f"Keeping file {f} because it's younger than daily oldest monthly date")
            continue

        if modifield_date >= oldest_yearly and modifield_date.day == 1 and modifield_date.month == 1:
            logger.info(f"Keeping file {f} because it's younger than daily oldest yearly date")
            continue

        to_delete.append(f)

    for f in to_delete:
        logger.info(f"Deleted file: {f}")
        f.unlink()

    for d in [d for d in backup_dir.glob('**/*') if d.is_dir()]:
        is_empty = not any(d.iterdir())
        if is_empty:
            logger.info(f"Deleted empty directory: {d}")
            os.rmdir(d)
        else:
            logger.info(f"Keeping directory: {d}")


    logger.info("_cleanup_old_backups: Ended")


servers = [
    {
        'conn_name': LIVE_DB_CONNECTION_NAME, # db02
        'exclude': {
            'information_schema',
            'mysql',
            'performance_schema',
            'reporting',
            'scratch',
            'sys',
            'uol_crf_redcap',
            'uol_easyas_redcap',
            'uol_openspecimen',
            'uol_survey_redcap',
        }
    },
    {
        'conn_name': OPS_DB_CONNECTION_NAME, # db01
        'exclude': {
            'briccs',
            'briccs_northampton',
            'civicrmlive_docker4716',
            'drupallive_docker4716',
            'identity',
            'information_schema',
            'mysql',
            'performance_schema',
            'redcap6170_briccs',
            'redcap6170_briccsext',
            'redcap_genvasc',
            'redcap_national',
            'redcap_test',
            'redcap_dev',
            'reporting',
            'scratch',
            'sys',
            'uol_crf_redcap',
            'uol_easyas_redcap',
            'uol_openspecimen',
            'uol_survey_redcap',
        }
    },
]


with DAG(
    dag_id="backup",
    schedule=SCHEDULE_BACKUP,
    default_args=default_dag_args,
):

    for s in servers:
        conn = MySqlConnection(s['conn_name'])

        with conn.query('SHOW DATABASES;') as cursor:
            for db, in cursor:
                if db not in s['exclude']:
                    _backup_database.override(task_id=f"backup_database__{s['conn_name']}__{db}")(conn=conn, db=db)

    _cleanup_old_backups()
