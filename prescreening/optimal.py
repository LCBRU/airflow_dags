from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.smtp.operators.smtp import EmailOperator

from tools import default_dag_args
from warehousing.schedules import SCHEDULE_PRESCREEING_OPTIMAL

with DAG(
    dag_id="prescreening_optimal",
    schedule=SCHEDULE_PRESCREEING_OPTIMAL,
    default_args=default_dag_args,
) as dag:

    search_optimal_study = SQLExecuteQueryOperator(
        task_id="search_optimal_study",
        conn_id="UHLDWH",
        sql="EXEC dwbriccs.dbo.LCBRU_search_Optimal_study;",
    )

    email_notification = EmailOperator(
        task_id="email_notification",
        to=["richard.bramley5@nhs.net"],
        from_email="richard.a.bramley@uhl-tr.nhs.uk",
        subject="Optimal Study Prescreening Completed",
        html_content=("<p>The optimal study prescreening has completed. Run the spreadsheet refresh data and email Lana.</p>"),
        conn_id="smtp_default",
    )

    search_optimal_study >> email_notification
