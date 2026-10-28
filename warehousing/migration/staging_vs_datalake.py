from airflow import DAG, task
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.smtp.operators.smtp import EmailOperator

from tools import default_dag_args, error_emails
from warehousing.schedules import SCHEDULE_BACKUP

STG_REDCAP_LOG_EVENTSPLITTED_QUERY = """
    SELECT log_event_id, event_id, ts, user as c, event, pk, description, legacy, change_reason, project_id, Field_name, value, descrip, keepone
    FROM stg_redcap.dbo.redcap_log_eventsplitted
"""


DATALAKE_REDCAP_LOG_EVENTSPLITTED_QUERY = """
    SELECT
        log_event_id,
        event_id,
        CONVERT(datetime, STUFF(STUFF(STUFF(STUFF(STUFF(CAST(ts AS varchar(14)),13,0,':'),11,0,':'),9,0,' '),7,0,'-'),5,0,'-')) as ts,
        user as c,
        event,
        pk,
        description,
        legacy,
        change_reason,
        project_id,
        Field_name,
        value,
        descrip,
        keepone
    FROM datalake_redcap6170_briccs.dbo.redcap_log_eventsplitted
"""


def except_query_has_differences(query1, query2):
    hook = DbApiHook.get_hook(conn_id="LEGACY_DWH")
    
    record = hook.get_first(f"""
        SELECT CASE WHEN EXISTS ({query1} EXCEPT {query2}) THEN 1 ELSE 0 END AS mismatch;
    """)

    if record is None:
        raise ValueError("Query returned no results.")

    return record[0] == 1


def queries_differ(query1, query2):
    return except_query_has_differences(query1, query2) or except_query_has_differences(query2, query1)


@task.branch
def should_send_email(target_dir: str) -> str:
    if queries_differ(STG_REDCAP_LOG_EVENTSPLITTED_QUERY, DATALAKE_REDCAP_LOG_EVENTSPLITTED_QUERY):
        return "send_email"
    else:
        return "no_email"

@task
def no_email() -> None:
    pass

def build_staging_vs_datalake_report_dag():
    with DAG(
        dag_id="staging_vs_datalake",
        default_args=default_dag_args,
        schedule=SCHEDULE_BACKUP,
    ) as dag:

        send_email = EmailOperator(
            task_id="send_email",
            to=error_emails,
            from_email="richard.a.bramley@uhl-tr.nhs.uk",
            subject="Differences Between DataLake and Staging",
            html_content=("<p>Differences Between DataLake and Staging for redcap_log_eventsplitted in REDCap UHL Internal.</p>"),
            conn_id="smtp_default",
        )

        should_send_email() >> [send_email, no_email()]

    return dag


build_staging_vs_datalake_report_dag()
