from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


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
