import os
from airflow.sdk import DAG, task
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.sdk import task
from airflow.utils.email import send_email
from tools import default_dag_args, error_emails
from warehousing.schedules import SCHEDULE_BACKUP


with DAG(
    dag_id="staging_vs_datalake",
    default_args=default_dag_args,
    schedule=SCHEDULE_BACKUP,
):

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
        hook.schema = "warehouse_central"
        
        record = hook.get_first(f"""
            SELECT CASE WHEN EXISTS ({query1} EXCEPT {query2}) THEN 1 ELSE 0 END AS mismatch;
        """)

        if record is None:
            raise ValueError("Query returned no results.")

        return record[0] == 1

    def queries_differ(query1, query2):
        return except_query_has_differences(query1, query2) or except_query_has_differences(query2, query1)
    
    @task
    def staging_vs_datalake_report():
        if queries_differ(STG_REDCAP_LOG_EVENTSPLITTED_QUERY, DATALAKE_REDCAP_LOG_EVENTSPLITTED_QUERY):
            html = """
            <html>
            <body>
                <h1>Differences Between DataLake and Staging for redcap_log_eventsplitted in REDCap UHL Internal</h1>
            </body>
            </html>
            """
            
            send_email(
                to=error_emails,
                subject="Differences Between DataLake and Staging",
                html_content=html,
            )

    staging_vs_datalake_report()
