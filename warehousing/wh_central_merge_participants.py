import logging
from tools import create_sub_dag_task, execute_mssql
from airflow.operators.python_operator import PythonOperator

DWH_CONNECTION_NAME = 'DWH'


def _merge_participants():
    logging.info("_create_views: Started")

    execute_mssql(
        DWH_CONNECTION_NAME,
        schema='warehouse_central',
        file_path=f'wh_central_merge_data/cleanup/DROP__Participants.sql',
    )

    for sql_file in [
        'CREATE__wh_participants.sql',
        'INSERT__wh_participants__redcap.sql',
        'INSERT__wh_participants__openspecimen.sql',
        'INSERT__wh_participants__table_columns.sql',
        'MERGE__wh_participants.sql',
    ]:
        logging.info(f'Running: {sql_file}')

        execute_mssql(
            DWH_CONNECTION_NAME,
            schema='warehouse_central',
            file_path=f'wh_central_merge_data/participants/{sql_file}',
        )

    logging.info("_create_views: Ended")


def create_wh_central_merge_participants(dag):
    parent_subdag = create_sub_dag_task(dag, 'wh_central_merge_participants')

    PythonOperator(
        task_id="merge_participants",
        python_callable=_merge_participants,
        dag=parent_subdag.subdag,
    )

    return parent_subdag
