from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.sdk import DAG, task

from tools import default_dag_args, error_emails
from warehousing.schedules import SCHEDULE_BACKUP

STG_REDCAP_STANDARD_QUERY = """
    SELECT *
    FROM {database}.dbo.{table}
"""


STG_REDCAP_LOG_EVENTSPLITTED_QUERY = """
    SELECT log_event_id, event_id, ts, user as c, event, pk, description, legacy, change_reason, project_id, Field_name, value, descrip, keepone
    FROM {database}.dbo.{table}
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
    FROM {database}.dbo.{table}
"""

TABLE_QUERIES = {
    "redcap_log_eventsplitted": {
        "staging_query": STG_REDCAP_LOG_EVENTSPLITTED_QUERY,
        "datalake_query": DATALAKE_REDCAP_LOG_EVENTSPLITTED_QUERY,
    },
    "redcap_metadata": {
        "staging_query": STG_REDCAP_STANDARD_QUERY,
        "datalake_query": STG_REDCAP_STANDARD_QUERY,
    }
}

DATABASE_XREF = {
    "redcap_uol": {
        "staging_database": "stg_uol_crf_redcap",
        "datalake_database": "datalake_uol_crf_redcap",
    },
    "redcap_uhl": {
        "staging_database": "stg_redcap",
        "datalake_database": "datalake_redcap6170_briccs",
    },
    "redcap_survey": {
        "staging_database": "stg_redcap_survey",
        "datalake_database": "datalake_uol_survey_redcap",
    }
}

def get_queries_text(database: str, table: str) -> tuple[str, str]:
    if table in TABLE_QUERIES:
        return (
            TABLE_QUERIES[table]["staging_query"].format(
                database=DATABASE_XREF[database]["staging_database"],
                table=table
            ),
            TABLE_QUERIES[table]["datalake_query"].format(
                database=DATABASE_XREF[database]["datalake_database"],
                table=table
            )
        )
    else:
        raise ValueError(f"No queries defined for {database}.{table}")

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
def should_send_email(queries: tuple[str, str]) -> str:
    if queries_differ(queries[0], queries[1]):
        return "send_email"
    else:
        return "no_email"

@task
def no_email() -> None:
    pass

def build_staging_vs_datalake_report_dag(database: str, table: str) -> DAG:
    with DAG(
        dag_id=f"staging_vs_datalake_{database}_{table}",
        default_args=default_dag_args,
        schedule=SCHEDULE_BACKUP,
    ) as dag:

        send_email = EmailOperator(
            task_id="send_email",
            to=error_emails,
            from_email="richard.a.bramley@uhl-tr.nhs.uk",
            subject="Differences Between DataLake and Staging",
            html_content=(f"<p>Differences Between DataLake and Staging for {database}.{table}.</p>"),
            conn_id="smtp_default",
        )

        should_send_email(get_queries_text(database, table)) >> [send_email, no_email()]

    return dag


build_staging_vs_datalake_report_dag("redcap_uhl", "redcap_log_eventsplitted")
build_staging_vs_datalake_report_dag("redcap_survey", "redcap_log_eventsplitted")
build_staging_vs_datalake_report_dag("redcap_uol", "redcap_log_eventsplitted")

build_staging_vs_datalake_report_dag("redcap_uhl", "redcap_metadata")
