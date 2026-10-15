from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from tools import default_dag_args
from warehousing.schedules import SCHEDULE_PRESCREEING

with DAG(
    dag_id="load_patient_data",
    schedule=SCHEDULE_PRESCREEING,
    default_args=default_dag_args,
) as dag:

    load_patients_from_dwrad = SQLExecuteQueryOperator(
        task_id="load_patients_from_dwrad",
        conn_id="UHLDWH",
        sql="EXEC dwbriccs.brc.load_patients_dwrad;",
    )

    load_patients_from_dwrepo = SQLExecuteQueryOperator(
        task_id="load_patients_from_dwrepo",
        conn_id="UHLDWH",
        sql="EXEC dwbriccs.brc.load_patients_dwrepo;",
    )

    load_patient_details = SQLExecuteQueryOperator(
        task_id="load_patient_details",
        conn_id="UHLDWH",
        sql="EXEC dwbriccs.brc.load_patient_details;",
    )

    load_patients_from_dwrad >> load_patients_from_dwrepo >> load_patient_details
