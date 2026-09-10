import os
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from tools import default_dag_args
import warehousing.migration.reports
import warehousing.migration.schema_export


with DAG(
    dag_id="migration_processing",
    default_args=default_dag_args,
    schedule=os.environ.get('SCHEDULE_BACKUP', None) or None,
):
    legacydwh_update_databases = SQLExecuteQueryOperator(
        task_id="legacy_update_databases",
        conn_id="LEGACY_DWH",
        database="warehouse_central",
        sql="EXEC mig__update_databases;",
    )

    dwh_update_databases = SQLExecuteQueryOperator(
        task_id="new_update_databases",
        conn_id="DWH",
        database="warehouse_central",
        sql="EXEC mig__update_databases;",
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
