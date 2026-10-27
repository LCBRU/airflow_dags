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
    
    @task
    def staging_vs_datalake_report():
        hook = DbApiHook.get_hook(conn_id="LEGACY_DWH")
        hook.schema = "warehouse_central"
        
        records = hook.get_records("""
            SELECT TOP 1 'Differences Found' as status
            FROM (
            SELECT log_event_id, event_id, ts, user as u, event, pk, description, legacy, change_reason, project_id, Field_name, value, descrip, keepone
            FROM stg_redcap.dbo.redcap_log_eventsplitted
            EXCEPT
            SELECT log_event_id, event_id, CONVERT(datetime,
                STUFF(STUFF(STUFF(STUFF(STUFF(
                CAST(ts AS varchar(14)),
                13,0,':'),
                11,0,':'),
                9,0,' '),
                7,0,'-'),
                5,0,'-')) AS ts
            , user, event, pk, description, legacy, change_reason, project_id, Field_name, value, descrip, keepone
            FROM datalake_redcap6170_briccs.dbo.redcap_log_eventsplitted

            UNION ALL

            SELECT log_event_id, event_id, CONVERT(datetime,
                STUFF(STUFF(STUFF(STUFF(STUFF(
                CAST(ts AS varchar(14)),
                13,0,':'),
                11,0,':'),
                9,0,' '),
                7,0,'-'),
                5,0,'-')) AS ts
            , user, event, pk, description, legacy, change_reason, project_id, Field_name, value, descrip, keepone
            FROM datalake_redcap6170_briccs.dbo.redcap_log_eventsplitted
            EXCEPT
            SELECT log_event_id, event_id, ts, user, event, pk, description, legacy, change_reason, project_id, Field_name, value, descrip, keepone
            FROM stg_redcap.dbo.redcap_log_eventsplitted

            ) x
        """)
        
        html = """
        <html>
        <body>
            <h1>Differences Between DataLake and Staging for redcap_log_eventsplitted in REDCap UHL Internal</h1>
            <table>
                <tr><th>Database Name</th><th>Status</th></tr>
            </table>
        </body>
        </html>
        """
        
        for row in records:
            html += f"<tr><td>{row[0]}</td><td>{row[1]}</td></tr>"
        
        html += "</table></body></html>"

        send_email(
            to=error_emails,
            subject="Differences Between DataLake and Staging",
            html_content=html,
        )

    staging_vs_datalake_report()
