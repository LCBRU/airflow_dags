import logging
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator
from tools import create_sub_dag_task, execute_mssql

DWH_CONNECTION_NAME = 'DWH'


def _create_views():
    logging.info("_create_views: Started")

    for sql_file in [
        'CREATE_VIEW__merged__redcap_data.sql',
        'CREATE_VIEW__merged__redcap_metadata.sql',
        'CREATE_VIEW__merged__redcap_project.sql',
        'CREATE_VIEW__etl__redcap_project_mapping.sql',
        'CREATE_VIEW__etl__openspecimen_mapping.sql',
    ]:
        logging.info(f'Running: {sql_file}')

        execute_mssql(
            DWH_CONNECTION_NAME,
            schema='warehouse_central',
            file_path=f'wh_create_views/{sql_file}',
        )

    logging.info("_create_views: Ended")


def _create_ddl(dag):
    logging.info("_create_views: Started")

    create_views = PythonOperator(
        task_id="create_views",
        python_callable=_create_views,
        dag=dag,
    )

    logging.info("_create_views: Ended")


def create_wh_create_premerge_views(dag):
    parent_subdag = create_sub_dag_task(dag, 'wh_create_premerge_views')

    _create_ddl(dag=parent_subdag.subdag)

    return parent_subdag