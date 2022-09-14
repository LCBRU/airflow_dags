import logging
from airflow import DAG
from airflow.operators.subdag import SubDagOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from contextlib import contextmanager
from warehousing import sql_path


def create_sub_dag_task(dag, sub_task_id, run_on_failures=False):
    subdag = DAG(
        dag_id=f"{dag.dag_id}.{sub_task_id}",
        default_args=dag.default_args,
    )

    params = {}

    if run_on_failures:
        params['trigger_rule'] = 'all_done'

    return SubDagOperator(
        task_id=sub_task_id,
        subdag=subdag,
        default_args=dag.default_args,
        dag=dag,
        **params,
    )


@contextmanager
def query_mssql(connection_name, schema=None, sql=None, file_path=None, parameters=None):
    logging.info("query_mssql: Started")

    if not parameters:
        parameters = {}

    mysql = MsSqlHook(mssql_conn_id=connection_name, schema=schema)
    conn = mysql.get_conn()
    cursor = conn.cursor()

    if sql is not None:
        cursor.execute(sql, parameters)
    elif file_path is not None:
        with open(sql_path() / file_path) as sql_file:
            cursor.execute(sql_file.read(), parameters)

    try:
        yield cursor

    finally:
        cursor.close()
        conn.close()

        logging.info("query_mssql: Ended")


def execute_mssql(connection_name, schema=None, sql=None, file_path=None, parameters=None):
    logging.info("execute_mssql: Started")

    mysql = MsSqlHook(mssql_conn_id=connection_name, schema=schema)
    conn = mysql.get_conn()
    cursor = conn.cursor()

    if sql is not None:
        cursor.execute(sql, parameters)
    elif file_path is not None:
        with open(sql_path() / file_path) as sql_file:
            cursor.execute(sql_file.read(), parameters)

    conn.commit()
    cursor.close()
    conn.close()

    logging.info("execute_mssql: Ended")


