import fileinput
import gzip
import os
import re
import subprocess
import tempfile
import requests
import shutil
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from itertools import groupby
from airflow.models import Variable


LIVE_DB_CONNECTION_NAME = 'LIVE_DB'
CUR_DIR = os.path.abspath(os.path.dirname(__file__))


def download_and_restore(destination_database, source_url):
    downloaded_filename = tempfile.NamedTemporaryFile()
    decrypted_filename = tempfile.NamedTemporaryFile()
    unzipped_filename = tempfile.NamedTemporaryFile()

    download_file(
        url=source_url,
        output_filename=downloaded_filename.name,
        username=Variable.get("ETL_DOWNLOAD_USERNAME"),
        password=Variable.get("ETL_DOWNLOAD_PASSWORD"),
    )

    decrypt_file(
        input_filename=downloaded_filename.name,
        output_filename=decrypted_filename.name,
        password=Variable.get("ETL_ENCRYPTION_PASSWORD"),
    )

    unzip_file(
        input_filename=decrypted_filename.name,
        output_filename=unzipped_filename.name,
    )

    amend_database_name(
        input_filename=unzipped_filename.name,
    )

    drop_database(destination_database)
    create_database(destination_database)
    restore_database(destination_database, unzipped_filename.name)

    downloaded_filename.close()
    decrypted_filename.close()
    unzipped_filename.close()


def download_file(url, output_filename, username, password):
    print("Downloading files")

    with requests.get(url, stream=True, auth=(username, password)) as r:
        with open(output_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    print("Done downloading files")


def decrypt_file(input_filename, output_filename, password):
    print("Decrypting files")

    proc = subprocess.run([
        'gpg',
        '--decrypt',
        '--batch',
        '--output',
        output_filename,
        '--passphrase',
        password,
        '--no-use-agent',
        '--yes',
        input_filename,
    ])

    if proc.returncode != 0:
        raise Exception('Could not decrypt file error code = {}.'.format(proc.returncode))

    print("Done decrypting files")


def unzip_file(input_filename, output_filename):
    print("Unzipping files")

    with gzip.open(input_filename, 'rb') as f_in:
        with open(output_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    print("Done unzipping files")


def amend_database_name(input_filename):
    print("Amending database name")

    create_db = re.compile('create\s*database', re.IGNORECASE)
    use_db = re.compile('use\s', re.IGNORECASE)

    with fileinput.FileInput(input_filename, inplace=True) as file:
        for line in file:
            if not create_db.match(line) and not use_db.match(line):
                processed = line
                processed = processed.lower()
                print(processed, end='')

    print("Done amending database name")


def drop_database(destination_database):
    sql = 'DROP DATABASE IF EXISTS {};'.format(destination_database)
    mysql = MySqlHook(mysql_conn_id=LIVE_DB_CONNECTION_NAME)
    conn = mysql.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)


def create_database(destination_database):
    sql = 'CREATE DATABASE {};'.format(destination_database)
    mysql = MySqlHook(mysql_conn_id=LIVE_DB_CONNECTION_NAME)
    conn = mysql.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)


def restore_database(destination_database, input_filename):
    proc = run_mysql('USE {};\nSOURCE {}'.format(
        destination_database,
        input_filename,
    ))

    if proc.returncode != 0:
        raise Exception('Could not restore database "{}" (ERROR: {})'.format(
            destination_database,
            proc.returncode,
        ))


def run_mysql(command):
    mysqlhook = MySqlHook(mysql_conn_id=LIVE_DB_CONNECTION_NAME)
    conn = mysqlhook.get_connection(LIVE_DB_CONNECTION_NAME)

    result = subprocess.run(
        [
            'mysql',
            '-h',
            conn.host,
            '-u',
            conn.login,
            '--password={}'.format(conn.password),
            '-e',
            command,
        ],
        capture_output=True,
    )

    print(result.stdout)
    print(result.stderr)

    return result


def create_dag_task(dag, source_url, destination_database):
    sub_task_id=f'download_and_restore_{destination_database}'

    subdag = DAG(
        dag_id=f"{dag.dag_id}.{sub_task_id}",
        default_args=dag.default_args,
    )

    PythonOperator(
        task_id="download_and_restore",
        python_callable=download_and_restore,
        dag=subdag,
        op_kwargs={
            'destination_database': destination_database,
            'source_url': source_url,
        },
    )

    SubDagOperator(
        task_id=sub_task_id,
        subdag=subdag,
        default_args=dag.default_args,
        dag=dag,
    )


default_args = {
    "owner": "airflow",
    "reties": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2020, 1, 1)
}


dag = DAG(
    dag_id="download_to_mysql",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
)


create_dag_task(
    dag=dag,
    source_url='https://crf.lcbru.le.ac.uk/publish/redcap.db',
    destination_database='uol_crf_redcap',
)
