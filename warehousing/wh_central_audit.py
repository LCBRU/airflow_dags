import logging
from pathlib import Path
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from tools import create_sub_dag_task, execute_mssql, query_mssql_dict
from jinja2 import Environment, FileSystemLoader
from warehousing import sql_path


DWH_CONNECTION_NAME = 'DWH'


def _log_dq_errors(**kwargs):
    logging.info("_log_dq_errors: Started")

    run_id = kwargs['dag_run'].run_id

    for f in (sql_path() / 'wh_central_merge_data/audit').iterdir():
        if f.is_dir():
            _run_dq_error(run_id, f)    

    logging.info("_log_dq_errors: Ended")


def _run_dq_error(run_id, folder):
    logging.info("_run_dq_error: Started")

    environment = Environment(loader=FileSystemLoader(folder))
    template = environment.get_template('template.j2')

    with query_mssql_dict(DWH_CONNECTION_NAME, schema='warehouse_central', file_path=folder / 'query.sql') as cursor:
        errors = template.render(cursor=list(cursor))

    sql__insert = '''
        INSERT INTO etl_errors (run_id, title, error)
        VALUES (%(run_id)s, 'REDCap Projects Unmapped', %(error)s)
    '''

    if len(errors) > 0:
        execute_mssql(
            DWH_CONNECTION_NAME,
            schema='warehouse_central',
            sql=sql__insert,
            parameters={
                'run_id': run_id,
                'error': errors,
            },
        )

    logging.info("_run_dq_error: Ended")


def _send_email(**kwargs):

    logging.info("_send_email: Started")

    sql__errors = '''
        SELECT *
        FROM etl_errors
        WHERE run_id = %(run_id)s
    '''

    lines = []

    with query_mssql_dict(
        DWH_CONNECTION_NAME,
        schema='warehouse_central',
        sql=sql__errors,
        parameters={'run_id': kwargs['dag_run'].run_id},
    ) as cursor:

        lines.append('''
<style>
    dl {
        display: grid;
        grid-template-columns: 1fr 2fr;
    }
    dt {
        font-weight: bold;
    }
</style>
        ''')
        lines.append("<h1>Data Warehousing Errors</h1>\n")
        lines.append("<p>The following errors were raised during data warehousing.</p>\n")
        lines.extend([r['error'] for r in cursor])

    send_email(
        html_content='\n'.join(lines),
        to=["richard.a.bramley@uhl-tr.nhs.uk"],
        subject="Data Warehousing Errors",
    )

    logging.info("_send_email: Ended")


def create_wh_central_audit(dag):
    parent_subdag = create_sub_dag_task(dag, 'wh_central_audit')

    log_dq_errors = PythonOperator(
        task_id="log_dq_errors",
        python_callable=_log_dq_errors,
        dag=parent_subdag.subdag,
    )

    send_email = PythonOperator(
        task_id="_send_email",
        python_callable=_send_email,
        dag=parent_subdag.subdag,
    )

    log_dq_errors >> send_email

    return parent_subdag
