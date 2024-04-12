import logging
import os
import pathlib
from datetime import datetime
import subprocess
from airflow import DAG
from tools import default_dag_args
from warehousing.database import LiveDbConnection, ReplicantDbConnection
from airflow.operators.python_operator import PythonOperator


BACKUP_DIRECTORY = '/backup/live_db/'


def _backup_database(db):
    logging.info("_backup_database: Started")

    conn = LiveDbConnection(db)

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

    backup_dir = pathlib.Path(BACKUP_DIRECTORY)
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


exclude = {
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

with DAG(
    dag_id="backup",
    schedule_interval=os.environ.get('SCHEDULE_REPLICATE', None) or None,
    default_args=default_dag_args,
    catchup=False,
    start_date=datetime(2020, 1, 1),
):

    master = LiveDbConnection()

    with master.query('SHOW DATABASES;') as cursor:
        for db, in cursor:
            if db not in exclude:
                PythonOperator(
                    task_id=f"backup_database_{db}",
                    python_callable=_backup_database,
                    op_kwargs={'db': db},
                )
