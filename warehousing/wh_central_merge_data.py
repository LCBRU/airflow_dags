import logging
from airflow.operators.mssql_operator import MsSqlOperator
from tools import create_sub_dag_task

DWH_CONNECTION_NAME = 'DWH'


def _create_merge_meta_data_dag(dag):
    logging.info("_create_merge_data_dag: Started")

    parent_subdag = create_sub_dag_task(dag, 'merge_meta_data')

    drop__meta_redcap_tables = MsSqlOperator(
        task_id='DROP__meta_redcap_tables',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/DROP__meta_redcap_tables.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__meta_redcap_data_type = MsSqlOperator(
        task_id='create__meta_redcap_data_type',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__meta_redcap_data_type.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__meta_redcap_instance = MsSqlOperator(
        task_id='CREATE__meta_redcap_instance',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__meta_redcap_instance.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__meta_redcap_project = MsSqlOperator(
        task_id='CREATE__meta_redcap_project',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__meta_redcap_project.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__meta_redcap_form = MsSqlOperator(
        task_id='CREATE__meta_redcap_form',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__meta_redcap_form.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__meta_redcap_form_section = MsSqlOperator(
        task_id='CREATE__meta_redcap_form_section',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__meta_redcap_form_section.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__meta_redcap_field = MsSqlOperator(
        task_id='CREATE__meta_redcap_field',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__meta_redcap_field.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__meta_redcap_field_enum = MsSqlOperator(
        task_id='CREATE__meta_redcap_field_enum',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__meta_redcap_field_enum.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__meta_redcap_arm = MsSqlOperator(
        task_id='CREATE__meta_redcap_arm',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__meta_redcap_arm.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__meta_redcap_event = MsSqlOperator(
        task_id='CREATE__meta_redcap_event',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__meta_redcap_event.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    drop__meta_redcap_tables >> create__meta_redcap_data_type >> create__meta_redcap_instance >> create__meta_redcap_project >> create__meta_redcap_form >> create__meta_redcap_form_section >> create__meta_redcap_field >> create__meta_redcap_field_enum
    create__meta_redcap_project >> create__meta_redcap_arm >> create__meta_redcap_event

    logging.info("_create_merge_data_dag: Ended")

    return parent_subdag


def _create_merge_participant_dag(dag):
    logging.info("_create_merge_participant_dag: Started")

    parent_subdag = create_sub_dag_task(dag, 'merge_participants')

    create__redcap_participant = MsSqlOperator(
        task_id='CREATE__redcap_participant',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__redcap_participant.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    logging.info("_create_merge_participant_dag: Ended")

    return parent_subdag


def _create_merge_file_dag(dag):
    logging.info("_create_merge_file_dag: Started")

    parent_subdag = create_sub_dag_task(dag, 'merge_files')

    create__redcap_file = MsSqlOperator(
        task_id='CREATE__redcap_file',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__redcap_file.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    logging.info("_create_merge_file_dag: Ended")

    return parent_subdag


def _create_merge_log_dag(dag):
    logging.info("_create_merge_log_dag: Started")

    parent_subdag = create_sub_dag_task(dag, 'merge_log')

    create__redcap_log = MsSqlOperator(
        task_id='create__redcap_log',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__redcap_log.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__redcap_log__unique = MsSqlOperator(
        task_id='create__redcap_log__unique',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__redcap_log__unique.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__redcap_log >> create__redcap_log__unique

    logging.info("_create_merge_log_dag: Ended")

    return parent_subdag


def _create_merge_data_dag(dag):
    logging.info("_create_merge_data_dag: Started")

    parent_subdag = create_sub_dag_task(dag, 'merge_data')

    create__redcap_data = MsSqlOperator(
        task_id='CREATE__redcap_data',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__redcap_data.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__redcap_data__unique = MsSqlOperator(
        task_id='create__redcap_data__unique',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/CREATE__redcap_data__unique.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create__redcap_data >> create__redcap_data__unique

    logging.info("_create_merge_data_dag: Ended")

    return parent_subdag


def _create_update_statistics_dag(dag):
    logging.info("_create_update_statistics_dag: Started")

    parent_subdag = create_sub_dag_task(dag, 'update_statistics')

    update_statistics = MsSqlOperator(
        task_id='update_statistics',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/UPDATE_STATISTICS.sql",
        autocommit=True,
        database='warehouse_central',
        trigger_rule="all_done",
        dag=parent_subdag.subdag,
    )

    logging.info("_create_update_statistics_dag: Ended")

    return parent_subdag


def create_wh_central_merge_data_dag(dag):
    parent_subdag = create_sub_dag_task(dag, 'wh_central_merge_data')

    meta = _create_merge_meta_data_dag(dag=parent_subdag.subdag)
    participants = _create_merge_participant_dag(dag=parent_subdag.subdag)
    files = _create_merge_file_dag(dag=parent_subdag.subdag)
    logs = _create_merge_log_dag(dag=parent_subdag.subdag)
    data = _create_merge_data_dag(dag=parent_subdag.subdag)
    stats = _create_update_statistics_dag(dag=parent_subdag.subdag)

    meta >> participants >> files >> data >> logs >> stats

    return parent_subdag