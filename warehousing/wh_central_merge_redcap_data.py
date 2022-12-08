import logging
from airflow.operators.mssql_operator import MsSqlOperator
from tools import create_sub_dag_task

DWH_CONNECTION_NAME = 'DWH'


def _create_merge_redcap_metadata_dag(dag):
    logging.info("_create_merge_redcap_data_dag: Started")

    parent_subdag = create_sub_dag_task(dag, 'merge_metadata', run_on_failures=True)

    create__meta_redcap_data_type = MsSqlOperator(
        task_id='CREATE__meta_redcap_data_type',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/redcap/CREATE__meta_redcap_data_type.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__meta_redcap_project = MsSqlOperator(
        task_id='CREATE__meta_redcap_project',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/redcap/CREATE__meta_redcap_project.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__meta_redcap_arm = MsSqlOperator(
        task_id='CREATE__meta_redcap_arm',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/redcap/CREATE__meta_redcap_arm.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__meta_redcap_event = MsSqlOperator(
        task_id='CREATE__meta_redcap_event',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/redcap/CREATE__meta_redcap_event.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__meta_redcap_form = MsSqlOperator(
        task_id='CREATE__meta_redcap_form',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/redcap/CREATE__meta_redcap_form.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__meta_redcap_form_section = MsSqlOperator(
        task_id='CREATE__meta_redcap_form_section',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/redcap/CREATE__meta_redcap_form_section.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__meta_redcap_field = MsSqlOperator(
        task_id='CREATE__meta_redcap_field',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/redcap/CREATE__meta_redcap_field.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__meta_redcap_field_enum = MsSqlOperator(
        task_id='CREATE__meta_redcap_field_enum',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/redcap/CREATE__meta_redcap_field_enum.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__meta_redcap_data_type >> create__meta_redcap_field
    create__meta_redcap_project >> create__meta_redcap_form >> create__meta_redcap_form_section >> create__meta_redcap_field >> create__meta_redcap_field_enum
    create__meta_redcap_project >> create__meta_redcap_arm >> create__meta_redcap_event

    logging.info("_create_merge_redcap_data_dag: Ended")

    return parent_subdag


def _create_merge_redcap_data_dag(dag):
    logging.info("_create_merge_redcap_data_dag: Started")

    parent_subdag = create_sub_dag_task(dag, 'merge_data', run_on_failures=True)

    redcap_participant = MsSqlOperator(
        task_id='CREATE__redcap_participant',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/redcap/CREATE__redcap_participant.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    redcap_file = MsSqlOperator(
        task_id='CREATE__redcap_file',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/redcap/CREATE__redcap_file.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    redcap_data = MsSqlOperator(
        task_id='CREATE__redcap_data',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/redcap/CREATE__redcap_data.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    redcap_data__unique = MsSqlOperator(
        task_id='CREATE__redcap_data__unique',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/redcap/CREATE__redcap_data__unique.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    redcap_log = MsSqlOperator(
        task_id='CREATE__redcap_log',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/redcap/CREATE__redcap_log.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    redcap_log__unique = MsSqlOperator(
        task_id='CREATE__redcap_log__unique',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/redcap/CREATE__redcap_log__unique.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    redcap_participant >> redcap_file >> redcap_data >> redcap_data__unique >> redcap_log >> redcap_log__unique

    logging.info("_create_merge_redcap_data_dag: Ended")

    return parent_subdag


def _create_merge_redcap_participants_dag(dag):
    logging.info("_create_merge_redcap_participants_dag: Started")

    redcap__merge_participant = MsSqlOperator(
        task_id='INSERT__redcap_project_participant_identifier',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/redcap/INSERT__redcap_project_participant_identifier.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )
    logging.info("_create_merge_redcap_participants_dag: Ended")

    return redcap__merge_participant


def create_wh_central_merge_redcap_data_dag(dag):
    parent_subdag = create_sub_dag_task(dag, 'wh_central_merge_redcap_data')

    metadata = _create_merge_redcap_metadata_dag(dag=parent_subdag.subdag)
    data = _create_merge_redcap_data_dag(dag=parent_subdag.subdag)
    participant_merge = _create_merge_redcap_participants_dag(dag=parent_subdag.subdag)

    metadata >> data >> participant_merge

    return parent_subdag
