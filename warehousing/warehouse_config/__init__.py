import logging
from pathlib import Path
from tools import create_sub_dag_task, execute_mssql
from airflow.operators.python_operator import PythonOperator

DWH_CONNECTION_NAME = 'DWH'


def _create_config():
    logging.info("_create_config: Started")

    for sql_file in [
        'CREATE__cfg_study.sql',
        'CREATE__cfg_redcap_instance.sql',
        'CREATE__cfg_redcap_mapping.sql',
        'CREATE__etl_errors.sql',
        'CREATE__cfg_participant_identifier_type.sql',
        'CREATE__cfg_participant_source.sql',
        'CREATE__cfg_participant_identifier_table_columns.sql',
        'CREATE__cfg_openspecimen_study_mapping.sql',
    ]:
        logging.info(f'Running: {sql_file}')

        execute_mssql(
            DWH_CONNECTION_NAME,
            schema='warehouse_central',
            file_path=Path(__file__).parent.absolute() / 'sql' / sql_file,
        )

    logging.info("_create_config: Ended")


def create_wh_central_config(dag):
    parent_subdag = create_sub_dag_task(dag, 'initialise_config')

    PythonOperator(
        task_id="create_config",
        python_callable=_create_config,
        dag=parent_subdag.subdag,
    )

    return parent_subdag
