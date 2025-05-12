import logging
import os
import pathlib
from datetime import datetime, date, timedelta, timezone
import subprocess
from airflow import DAG
from tools import default_dag_args
from warehousing.database import LIVE_DB_CONNECTION_NAME, OPS_DB_CONNECTION_NAME, MySqlConnection
from airflow.operators.python_operator import PythonOperator
from dateutil.relativedelta import relativedelta


BACKUP_DIRECTORY = '/backup/live_db/'


def _backup_database(conn, db):
    logging.info("_backup_database: Started")

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

    backup_dir = pathlib.Path(BACKUP_DIRECTORY) / f"{datetime.now():%Y%m%d}"
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

    output, errors = zip.communicate()
    logging.error(errors)
    logging.info("_backup_database: Ended")


def _cleanup_old_backups():
    logging.info("_cleanup_old_backups: Started")

    backup_dir = pathlib.Path(BACKUP_DIRECTORY)
    backup_dir.mkdir(parents=True, exist_ok=True)

    today = date.today()
    oldest_daily = today - timedelta(days=7)
    oldest_weekly = today - timedelta(weeks=4)
    oldest_monthly = today - relativedelta(months=12)
    oldest_yearly = today - relativedelta(years=5)

    logging.info(f"Oldest daily date: {oldest_daily!r}")
    logging.info(f"Oldest weekly date: {oldest_weekly!r}")
    logging.info(f"Oldest monthly date: {oldest_monthly!r}")
    logging.info(f"Oldest yearly date: {oldest_yearly!r}")

    to_delete = []

    for f in [f for f in backup_dir.glob('**/*') if f.is_file()]:
        modifield_date = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc).date()

        if modifield_date >= oldest_daily:
            logging.info(f"Keeping file {f} because it's younger than daily oldest daily date")
            continue
    
        if modifield_date >= oldest_weekly and modifield_date.weekday() == 0:
            logging.info(f"Keeping file {f} because it's younger than daily oldest weekly date")
            continue

        if modifield_date >= oldest_monthly and modifield_date.day == 1:
            logging.info(f"Keeping file {f} because it's younger than daily oldest monthly date")
            continue

        if modifield_date >= oldest_yearly and modifield_date.day == 1 and modifield_date.month == 1:
            logging.info(f"Keeping file {f} because it's younger than daily oldest yearly date")
            continue

        to_delete.append(f)

    for f in to_delete:
        logging.info(f"Deleted file: {f}")
        f.unlink()

    for d in [d for d in backup_dir.glob('**/*') if d.is_dir()]:
        is_empty = not any(d.iterdir())
        if is_empty:
            logging.info(f"Deleted empty directory: {d}")
            f.unlink()
        else:
            logging.info(f"Keeping directory: {d}")


    logging.info("_cleanup_old_backups: Ended")


servers = [
    {
        'conn_name': LIVE_DB_CONNECTION_NAME,
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
        'conn_name': OPS_DB_CONNECTION_NAME,
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
            'briccs',
            'briccs_northampton',
            'civicrmlive_docker4716',
            'drupallive_docker4716',
            'identity',
            'redcap6170_briccs',
            'redcap6170_briccsext',
            'redcap_genvasc',
            'redcap_national',
            'redcap_test',
            'redcap_dev',
            'uol_crf_redcap',
            'uol_easyas_redcap',
            'uol_openspecimen',
            'uol_survey_redcap',
        }
    },
]


with DAG(
    dag_id="backup",
    schedule_interval=os.environ.get('SCHEDULE_BACKUP', None) or None,
    default_args=default_dag_args,
    catchup=False,
    start_date=datetime(2020, 1, 1),
):

    for s in servers:
        conn = MySqlConnection(s['conn_name'])

        with conn.query('SHOW DATABASES;') as cursor:
            for db, in cursor:
                if db not in s['exclude']:
                    PythonOperator(
                        task_id=f"backup_database__{s['conn_name']}__{db}",
                        python_callable=_backup_database,
                        op_kwargs={
                            'conn': conn,
                            'db': db,
                        },
                    )

    PythonOperator(
        task_id="_cleanup_old_backups",
        python_callable=_cleanup_old_backups,
    )
