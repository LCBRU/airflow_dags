import logging
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator
from tools import create_sub_dag_task, execute_mssql

DWH_CONNECTION_NAME = 'DWH'


def _create_views():
    logging.info("_create_views: Started")

    for sql_file in [
        'CREATE_VIEW__desc__redcap_field.sql',
        'CREATE_VIEW__desc__redcap_log.sql',
        'CREATE_VIEW__desc__redcap_value.sql',
        'CREATE_VIEW__dq__redcap_projects_unmapped.sql',
        'CREATE_VIEW__dq__redcap_projects_without_ids.sql',
        'CREATE_VIEW__dq__redcap_value__duplicates.sql',
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

    create_study_db_name_function = MsSqlOperator(
        task_id='create_study_db_name_function',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_create_views/CREATE__study_database_name.sql",
        autocommit=True,
        dag=dag,
        database='warehouse_central',
    )

    create_views = PythonOperator(
        task_id="create_views",
        python_callable=_create_views,
        dag=dag,
    )

    create_study_db_name_function >> create_views

    logging.info("_create_views: Ended")


def create_wh_create_postmerge_views(dag):
    parent_subdag = create_sub_dag_task(dag, 'wh_create_postmerge_views')

    _create_ddl(dag=parent_subdag.subdag)

    return parent_subdag