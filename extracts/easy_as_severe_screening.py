import logging
from airflow.operators.mssql_operator import MsSqlOperator
from tools import create_sub_dag_task

DWH_CONNECTION_NAME = 'UHLDWH'


def _create_screening_dag(dag):
    logging.info("_create_screening_dag: Started")

    query__screening_extract = MsSqlOperator(
        task_id='query__screening_extract',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="SELECT * FROM DWBRICCS.[dbo].[LCBRU_Results_easy_AS_severe_list_a]",
        autocommit=True,
        database='dwbriccs',
        dag=dag,
    )

    logging.info("_create_screening_dag: Ended")


def create_easy_as_severe_screening_dag(dag):
    parent_subdag = create_sub_dag_task(dag, 'easy_as_severe_screening')

    _create_screening_dag(dag=parent_subdag.subdag)

    return parent_subdag