import os
import tempfile
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag import SubDagOperator
from itertools import groupby


DWH_CONNECTION_NAME = 'DWH'
CUR_DIR = os.path.abspath(os.path.dirname(__file__))


def download_and_restore(destination_database, source_url):
    downloaded_filename = tempfile.NamedTemporaryFile()
    decrypted_filename = tempfile.NamedTemporaryFile()
    unzipped_filename = tempfile.NamedTemporaryFile()

    # download_file(
    #     url=url,
    #     output_filename=downloaded_filename.name,
    #     username=url_username,
    #     password=url_password,
    #     )

    # decrypt_file(
    #     input_filename=downloaded_filename.name,
    #     output_filename=decrypted_filename.name,
    #     password=decrypt_password,
    # )

    # unzip_file(
    #     input_filename=decrypted_filename.name,
    #     output_filename=unzipped_filename.name,
    # )

    # amend_database_name(
    #     input_filename=unzipped_filename.name,
    # )

    # drop_database()
    # create_database()
    # restore_database(unzipped_filename.name)

    downloaded_filename.close()
    decrypted_filename.close()
    unzipped_filename.close()

# def download_file(url, output_filename, username, password):
#     with requests.get(url, stream=True, auth=(username, password)) as r:
#         with open(output_filename, 'wb') as f:
#             shutil.copyfileobj(r.raw, f)

# def decrypt_file(input_filename, output_filename, password):
#     proc = subprocess.run([
#         'gpg',
#         '--decrypt',
#         '--batch',
#         '--output',
#         output_filename,
#         '--passphrase',
#         password,
#         '--no-use-agent',
#         '--yes',
#         input_filename,
#     ])

#     if proc.returncode != 0:
#         raise Exception('Could not decrypt file error code = {}.'.format(proc.returncode))

# def unzip_file(input_filename, output_filename):
#     with gzip.open(input_filename, 'rb') as f_in:
#         with open(output_filename, 'wb') as f_out:
#             shutil.copyfileobj(f_in, f_out)

# def amend_database_name(input_filename):
#     create_db = re.compile('create\s*database', re.IGNORECASE)
#     use_db = re.compile('use\s', re.IGNORECASE)

#     with fileinput.FileInput(input_filename, inplace=True) as file:
#         for line in file:
#             if not create_db.match(line) and not use_db.match(line):
#                 processed = line
#                 if lowercase:
#                     processed = processed.lower()
#                 print(processed, end='')

# def create_database():
#     proc = run_mysql('CREATE DATABASE {};'.format(destination_database_name))

#     if proc.returncode != 0:
#         raise Exception('Could not create database "{}" (ERROR: {})'.format(
#             destination_database_name,
#             proc.returncode,
#         ))

# def drop_database():
#     proc = run_mysql('DROP DATABASE IF EXISTS {};'.format(destination_database_name))

#     if proc.returncode != 0:
#         raise Exception('Could not drop database "{}" (ERROR: {})'.format(
#             destination_database_name,
#             proc.returncode,
#             ))

# def run_mysql(command):
#     result = subprocess.run(
#         [
#             'mysql',
#             '-h',
#             database_host,
#             '-u',
#             database_user,
#             '--password={}'.format(database_password),
#             '-e',
#             command,
#         ],
#         capture_output=True,
#     )

#     print(result.stdout)
#     print(result.stderr)

#     return result

# def restore_database(input_filename):
#     proc = run_mysql('USE {};\nSOURCE {}'.format(
#         destination_database_name,
#         input_filename,
#     ))

#     if proc.returncode != 0:
#         raise Exception('Could not restore database "{}" (ERROR: {})'.format(
#             destination_database_name,
#             proc.returncode,
#         ))


def create_download_and_restore_dag(parent_dag_id, sub_task_id, source_url, destination_database, args):
    result = DAG(
        dag_id=f"{parent_dag_id}.{sub_task_id}",
        default_args=args,
    )

    PythonOperator(
        task_id="download_and_restore",
        python_callable=download_and_restore,
        dag=result,
        op_kwargs={
            'destination_database': destination_database,
            'url': source_url,
        },
    )

    return result

default_args = {
    "owner": "airflow",
    "reties": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2020, 1, 1)
}

def create_dag_task(dag, source_url, destination_database):
    task_id = f"{source_url}__to__{destination_database}"

    SubDagOperator(
        task_id=task_id,
        subdag=create_download_and_restore_dag(
            parent_dag_id=dag.dag_id,
            sub_task_id=task_id,
            source_url=source_url,
            destination_database=destination_database,
            args=dag.default_args
        ),
        default_args=dag.default_args,
        dag=dag,
    )


dag = DAG(
    dag_id="download_to_mysql",
    schedule_interval="None",
    default_args=default_args,
    catchup=False,
)

create_dag_task(dag, 'https://crf.lcbru.le.ac.uk/publish/redcap.db', 'uol_crf_redcap')
