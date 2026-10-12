from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


load_patients_from_dwrad = SQLExecuteQueryOperator(
    task_id="load_patients_from_dwrad",
    conn_id="DWH",
    sql="EXEC uhldwhsql.dwbriccs.brc.load_patients_dwrad;",
)
