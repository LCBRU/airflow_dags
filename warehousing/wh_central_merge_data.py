import logging
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python_operator import PythonOperator
from itertools import groupby
from warehousing.tools import create_sub_dag_task, sql_path

DWH_CONNECTION_NAME = 'DWH'


def _create_merge_data_dag(dag):
    logging.info("_create_merge_data_dag: Started")

    create__redcap_instances = MsSqlOperator(
        task_id='CREATE__redcap_instances',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__redcap_instances.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    insert__redcap_project_participant_identifier = MsSqlOperator(
        task_id='INSERT__redcap_project_participant_identifier',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/INSERT__redcap_project_participant_identifier.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    create_database__wh_study = MsSqlOperator(
        task_id='CREATE_DATABASE__WH_Study',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE_DATABASE__WH_Study.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    create__redcap_metatdata_tables = MsSqlOperator(
        task_id='CREATE__REDCap_metatdata_tables',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__REDCap_metatdata_tables.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    insert__redcap_project_participant_identifier >> create_database__wh_study
    create__redcap_instances >> create_database__wh_study
    create_database__wh_study >> create__redcap_metatdata_tables

    logging.info("_create_merge_data_dag: Ended")


def create_wh_central_merge_data_dag(dag):
    parent_subdag = create_sub_dag_task(dag, 'wh_central_merge_data')

    _create_merge_data_dag(dag=parent_subdag.subdag)

    return parent_subdag