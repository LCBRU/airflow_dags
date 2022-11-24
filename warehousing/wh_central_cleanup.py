import logging
from tools import create_sub_dag_task, execute_mssql
from airflow.operators.python_operator import PythonOperator

DWH_CONNECTION_NAME = 'DWH'


def _create_cleanup():
    logging.info("_create_cleanup: Started")

    for sql_file in [
        'DROP__Participants.sql',
        'DROP__Config.sql',
        'DROP__Civicrm.sql',
        'DROP__OpenSpecimen.sql',
        'DROP__meta_redcap_tables.sql',
    ]:
        logging.info(f'Running: {sql_file}')

        execute_mssql(
            DWH_CONNECTION_NAME,
            schema='warehouse_central',
            file_path=f'wh_central_merge_data/cleanup/{sql_file}',
        )

    logging.info("_create_cleanup: Ended")


def create_wh_central_cleanup(dag):
    parent_subdag = create_sub_dag_task(dag, 'wh_central_cleanup')

    PythonOperator(
        task_id="_create_cleanup",
        python_callable=_create_cleanup,
        dag=parent_subdag.subdag,
    )

    return parent_subdag
