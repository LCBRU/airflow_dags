import os
from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from tools import default_dag_args


with DAG(
    dag_id="Copy_live_DB_to_DWH",
    default_args=default_dag_args,
    # schedule=os.environ.get('SCHEDULE_DATALAKE_LOAD', None) or None,
    template_searchpath=['/opt/airflow/dags/warehousing/datalake_load/sql/'],
    start_date=datetime(2020, 1, 1),
    catchup=False,
):
    legacydwh_update_databases = SQLExecuteQueryOperator(
        task_id="legacy_update_databases",
        conn_id="LEGACY_DWH",
        database="warehouse_central",
        sql="EXEC mig__update_datebases;",
    )

    dwh_update_databases = SQLExecuteQueryOperator(
        task_id="new_update_databases",
        conn_id="DWH",
        database="warehouse_central",
        sql="EXEC mig__update_datebases;",
    )

    legacydwh_update_tables = SQLExecuteQueryOperator(
        task_id="legacy_update_tables",
        conn_id="LEGACY_DWH",
        database="warehouse_central",
        sql="EXEC mig__update_tables;",
    )

    dwh_update_tables = SQLExecuteQueryOperator(
        task_id="new_update_tables",
        conn_id="DWH",
        database="warehouse_central",
        sql="EXEC mig__update_tables;",
    )

    legacydwh_update_databases >> legacydwh_update_tables
    dwh_update_databases >> dwh_update_tables
