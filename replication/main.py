import logging
import os
from datetime import datetime
import subprocess
from airflow import DAG
from tools import default_dag_args
from warehousing.database import LiveDbConnection, ReplicantDbConnection
from airflow.operators.python_operator import PythonOperator



def _replicate_database(db, live_conn, replicant_conn):
    logging.info("_replicate_database: Started")

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
            '--databases',
            db,
        ],
        # capture_output=True,
        # bufsize=0,
        stdout=subprocess.PIPE,
        text=True,
    )

    # load = subprocess.Popen(
    #     [
    #         'mysql',
    #         '-h',
    #         replicant_conn.host,
    #         '-u',
    #         replicant_conn.login,
    #         f'--password={replicant_conn.password}',
    #     ],
    #     stdin=dump.stdout,
    #     stdout=subprocess.PIPE,
    #     text=True,
    # )

    # output, errors = load.communicate()
    # dump.stdout.close()

    dump.wait()

    output, errors = dump.communicate()

    print(f'errors=')
    print(f'output=')

    logging.info("_replicate_database: Ended")

dbs = {
    # 'Yakult',
    # 'briccs',
    # 'briccs_auditor_temp',
    # 'briccs_kettering',
    # 'briccs_northampton',
    # 'briccsids',
    'civicrmlive_docker4716',
    # 'dq_central',
    'drupallive_docker4716',
    # 'etl_central',
    # 'genvasc_gp_portal',
    # 'grafana',
    'identity',
    # 'image_study_merge',
    # 'mrbs',
    # 'onyx',
    'redcap6170_briccs',
    'redcap6170_briccsext',
    # 'redcap_dev',
    'redcap_genvasc',
    'redcap_national',
    # 'redcap_test',
    # 'reporting',
    # 'scratch',
}


dbs = {
    'redcap_genvasc',
}

with DAG(
    dag_id="replication",
    schedule_interval=os.environ.get('SCHEDULE_REPLICATE', None) or None,
    default_args=default_dag_args,
    catchup=False,
    start_date=datetime(2020, 1, 1),
):
    for db in dbs:
        live_conn = LiveDbConnection(db)
        replicant_conn = ReplicantDbConnection(db)

        PythonOperator(
            task_id=f"replicate__replicate_database__{db}",
            python_callable=_replicate_database,
            op_kwargs={
                'db': db,
                'live_conn': live_conn,
                'replicant_conn': replicant_conn,
            },
        )
