import fileinput
import gzip
from pathlib import Path
import re
import subprocess
import tempfile
import requests
import shutil
import logging
from airflow.models import Variable
from warehousing.database import ReplicantDbConnection


def download_mysql_backup_and_restore(destination_database, source_url):
    logging.info("_download_and_restore: Started")

    downloaded_filename = tempfile.NamedTemporaryFile()
    decrypted_filename = tempfile.NamedTemporaryFile()
    unzipped_filename = tempfile.NamedTemporaryFile()

    _download_file(
        url=source_url,
        output_filename=downloaded_filename.name,
        username=Variable.get("ETL_DOWNLOAD_USERNAME"),
        password=Variable.get("ETL_DOWNLOAD_PASSWORD"),
    )

    _decrypt_file(
        input_filename=downloaded_filename.name,
        output_filename=decrypted_filename.name,
        password=Variable.get("ETL_ENCRYPTION_PASSWORD"),
    )

    _unzip_file(
        input_filename=decrypted_filename.name,
        output_filename=unzipped_filename.name,
    )

    _amend_database_name(
        input_filename=unzipped_filename.name,
    )

    _drop_database(destination_database)
    _create_database(destination_database)
    _restore_database(destination_database, unzipped_filename.name)

    downloaded_filename.close()
    decrypted_filename.close()
    unzipped_filename.close()

    logging.info("_download_and_restore: Ended")


def _download_file(url, output_filename, username, password):
    logging.info("_download_file: Started")

    logging.info(f"Downloading: {url}")

    with requests.get(url, stream=True, auth=(username, password)) as r:
        logging.info(f"Response: {url} = {r.status_code}")

        with open(output_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    logging.info("_download_file: Ended")


def _decrypt_file(input_filename, output_filename, password):
    logging.info("_decrypt_file: Started")

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

    logging.info("_decrypt_file: Ended")


def _unzip_file(input_filename, output_filename):
    logging.info("_unzip_file: Started")

    with gzip.open(input_filename, 'rb') as f_in:
        with open(output_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    logging.info("_unzip_file: Ended")


def _amend_database_name(input_filename):
    logging.info("_amend_database_name: Started")

    create_db = re.compile('create\s*database', re.IGNORECASE)
    use_db = re.compile('use\s', re.IGNORECASE)

    with fileinput.FileInput(input_filename, inplace=True) as file:
        for line in file:
            if not create_db.match(line) and not use_db.match(line):
                processed = line
                processed = processed.lower()
                print(processed, end='')

    logging.info("_amend_database_name: Ended")


def _drop_database(destination_database):
    logging.info("_drop_database: Started")

    sql = 'DROP DATABASE IF EXISTS {};'.format(destination_database)
    conn = ReplicantDbConnection()
    conn.execute(sql)
    logging.info("_drop_database: Ended")


def _create_database(destination_database):
    logging.info("_create_database: Started")

    sql = 'CREATE DATABASE {};'.format(destination_database)
    conn = ReplicantDbConnection()
    conn.execute(sql)

    logging.info("_create_database: Ended")


def _restore_database(destination_database, input_filename):
    logging.info("_restore_database: Started")
    logging.info(f"File = {input_filename}")

    logging.info('****************************************************************')

    with Path(input_filename).open() as input:
        for _ in range(10):
            logging.info(input.readline())

    logging.info('****************************************************************')

    # proc = _run_mysql('USE {};\nSOURCE {}'.format(
    #     destination_database,
    #     input_filename,
    # ))

    # if proc.returncode != 0:
    #     raise Exception('Could not restore database "{}" (ERROR: {})'.format(
    #         destination_database,
    #         proc.returncode,
    #     ))

    logging.info("_restore_database: Ended")


def _run_mysql(command):
    conn = ReplicantDbConnection()

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

    return result
