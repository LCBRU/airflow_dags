import os
from airflow.sdk import DAG, task
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.sdk import task
from airflow.utils.email import send_email
from tools import default_dag_args, error_emails


with DAG(
    dag_id="migration_reports",
    default_args=default_dag_args,
    schedule=os.environ.get('SCHEDULE_BACKUP', None) or None,
    catchup=False,
):
    
    @task
    def database_mismatch_report():
        hook = DbApiHook.get_hook(conn_id="DWH")
        hook.schema = "warehouse_central"
        
        records = hook.get_records("""
            SELECT database_name, status
            FROM mig_report__database_mismatch
            ORDER BY database_name
        """)
        
        html = """
        <html>
        <body>
        <h1>Migration Database Mismatch Report</h1>
        <table>
            <tr><th>Database Name</th><th>Status</th></tr>
        """
        
        for row in records:
            html += f"<tr><td>{row[0]}</td><td>{row[1]}</td></tr>"
        
        html += "</table></body></html>"

        send_email(
            to=error_emails,
            subject="Migration Database Mismatch Report",
            html_content=html,
        )

    database_mismatch_report()
