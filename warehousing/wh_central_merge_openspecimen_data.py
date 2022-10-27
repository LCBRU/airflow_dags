import logging
from airflow.operators.mssql_operator import MsSqlOperator
from tools import create_sub_dag_task

DWH_CONNECTION_NAME = 'DWH'


def _create_merge_openspecimen_dag(dag):
    logging.info("_create_merge_openspecimen_dag: Started")

    drop__tables = MsSqlOperator(
        task_id='DROP__Tables',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/openspecimen/DROP__Tables.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    create__collection_protocol = MsSqlOperator(
        task_id='CREATE__Collection_Protocol',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/openspecimen/CREATE__Collection_Protocol.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    create__participant = MsSqlOperator(
        task_id='CREATE__participant',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/openspecimen/CREATE__Participant.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    create__registration = MsSqlOperator(
        task_id='CREATE__registration',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/openspecimen/CREATE__Registration.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    create__event = MsSqlOperator(
        task_id='CREATE__event',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/openspecimen/CREATE__Event.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    create__specimen_group = MsSqlOperator(
        task_id='CREATE__specimen_group',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/openspecimen/CREATE__Specimen_Group.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    create__specimen = MsSqlOperator(
        task_id='CREATE__specimen',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/openspecimen/CREATE__Specimen.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    drop__tables >> create__collection_protocol
    drop__tables >> create__participant
    create__collection_protocol >> create__registration >> create__specimen_group
    create__collection_protocol >> create__event >> create__specimen_group >> create__specimen
    create__specimen_group >> create__specimen

    logging.info("_create_merge_openspecimen_dag: Ended")


def create_wh_central_merge_openspecimen_data_dag(dag):
    parent_subdag = create_sub_dag_task(dag, 'wh_central_merge_openspecimen_data')

    _create_merge_openspecimen_dag(dag=parent_subdag.subdag)

    return parent_subdag
