import logging
from airflow.operators.mssql_operator import MsSqlOperator
from tools import create_sub_dag_task

DWH_CONNECTION_NAME = 'DWH'


def _create_merge_data_dag(dag):
    logging.info("_create_merge_data_dag: Started")

    drop__meta_redcap_tables = MsSqlOperator(
        task_id='DROP__meta_redcap_tables',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/DROP__meta_redcap_tables.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    create__meta_redcap_instance = MsSqlOperator(
        task_id='CREATE__meta_redcap_instance',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__meta_redcap_instance.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    create__meta_redcap_project = MsSqlOperator(
        task_id='CREATE__meta_redcap_project',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__meta_redcap_project.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    create__meta_redcap_form = MsSqlOperator(
        task_id='CREATE__meta_redcap_form',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__meta_redcap_form.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    create__meta_redcap_form_section = MsSqlOperator(
        task_id='CREATE__meta_redcap_form_section',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__meta_redcap_form_section.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    create__meta_redcap_field = MsSqlOperator(
        task_id='CREATE__meta_redcap_field',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__meta_redcap_field.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    create__meta_redcap_field_enum = MsSqlOperator(
        task_id='CREATE__meta_redcap_field_enum',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__meta_redcap_field_enum.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    drop__meta_redcap_tables >> create__meta_redcap_instance
    create__meta_redcap_instance >> create__meta_redcap_project
    create__meta_redcap_project >> create__meta_redcap_form
    create__meta_redcap_form >> create__meta_redcap_form_section
    create__meta_redcap_form_section >> create__meta_redcap_field
    create__meta_redcap_field >> create__meta_redcap_field_enum

    logging.info("_create_merge_data_dag: Ended")


def create_wh_central_merge_data_dag(dag):
    parent_subdag = create_sub_dag_task(dag, 'wh_central_merge_data')

    _create_merge_data_dag(dag=parent_subdag.subdag)

    return parent_subdag