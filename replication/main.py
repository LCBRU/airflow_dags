import logging
import os
from datetime import timedelta, datetime
import subprocess
from airflow import DAG
from tools import create_sub_dag_task
from warehousing.database import LiveDbConnection, ReplicantDbConnection
from airflow.operators.python_operator import PythonOperator



default_args = {
    "owner": "airflow",
    "reties": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2020, 1, 1),
	'email': os.environ.get('ERROR_EMAIL_ADDRESS', '').split(';'),
	'email_on_failure': True,
}

dag = DAG(
    dag_id="replication",
    schedule_interval=os.environ.get('SCHEDULE_REPLICATE', None) or None,
    default_args=default_args,
    catchup=False,
)

def _replicate_database(db):
    logging.info("_replicate_database: Started")

    live_conn = LiveDbConnection(db)

    dump = subprocess.Popen(
        [
            'mysqldump',
            '-h',
            live_conn.host,
            '-u',
            live_conn.login,
            f'--password={live_conn.password}',
            '--add-drop-database',
            '--column-statistics=0',
            '--compact',
            '--databases',
            db,
        ],
        bufsize=0,
        stdout=subprocess.PIPE,
    )

    replicant_conn = ReplicantDbConnection(db)

    load = subprocess.Popen(
        [
            'mysql',
            '-h',
            replicant_conn.host,
            '-u',
            replicant_conn.login,
            f'--password={replicant_conn.password}',
        ],
        stdin=dump.stdout,
        stdout=subprocess.PIPE,
    )

    dump.stdout.close()
    result = load.communicate()[0]

    logging.info("_replicate_database: Ended")

dbs = {
    'Yakult',
    'briccs',
    'briccs_auditor_temp',
    'briccs_kettering',
    'briccs_northampton',
    'briccsids',
    'civicrmlive_docker4716',
    'dq_central',
    'drupallive_docker4716',
    'etl_central',
    'genvasc_gp_portal',
    'grafana',
    'identity',
    'image_study_merge',
    'mrbs',
    'onyx',
    'redcap6170_briccs',
    'redcap6170_briccsext',
    'redcap_dev',
    'redcap_genvasc',
    'redcap_national',
    'redcap_test',
    'reporting',
    'scratch',
}

def create_download_to_mysql_dag(dag):
    parent_subdag = create_sub_dag_task(dag, 'replicate_mysql', run_on_failures=True)

    for db in dbs:
        PythonOperator(
            task_id=f"replicate__replicate_database__{db}",
            python_callable=_replicate_database,
            dag=parent_subdag.subdag,
            op_kwargs={
                'db': db,
            },
        )
    
    return parent_subdag

create_download_to_mysql_dag(dag)