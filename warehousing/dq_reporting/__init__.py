import logging
from pathlib import Path
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from tools import create_sub_dag_task
from warehousing.database import WarehouseCentralConnection
from jinja2 import Environment, FileSystemLoader



def _log_dq_errors(**kwargs):
    logging.info("_log_dq_errors: Started")

    run_id = kwargs['dag_run'].run_id
    ts = kwargs['dag_run'].logical_date

    for g in [
        'warehouse_central/initial',
        'warehouse_central/redcap_mappings',
        'warehouse_central',
    ]:
        for f in (Path(__file__).parent.absolute() / 'jobs' / g).iterdir():
            if f.is_dir():
                _run_dq_error(run_id, ts, f)

    logging.info("_log_dq_errors: Ended")


def _run_dq_error(run_id, ts, folder):
    logging.info("_run_dq_error: Started")

    if not((folder / 'query.sql').exists() and (folder / 'template.j2').exists()):
        logging.info(f"****** Skipping ****** {folder}")
        return

    logging.info(f"************ {folder}")

    environment = Environment(loader=FileSystemLoader(folder))
    template = environment.get_template('template.j2')

    conn = WarehouseCentralConnection()

    with conn.query_dict(file_path=folder / 'query.sql', parameters={'ts': ts}) as cursor:
        errors = template.render(cursor=list(cursor)).strip()

    sql__insert = '''
        INSERT INTO warehouse_config.dbo.etl_error (run_id, title, error)
        VALUES ((SELECT id FROM warehouse_config.dbo.etl_run WHERE dag_run_ts = %(ts)s), 'REDCap Projects Unmapped', %(error)s)
    '''

    if len(errors) > 0:
        conn.execute(
            sql=sql__insert,
            parameters={
                'ts': ts.isoformat(),
                'error': errors,
            },
        )

    logging.info("_run_dq_error: Ended")


def _send_email(**kwargs):

    logging.info("_send_email: Started")

    sql__errors = '''
        SELECT *
        FROM warehouse_config.dbo.etl_error e
        JOIN warehouse_config.dbo.etl_run r
            ON r.dag_run_ts = %(ts)s
            AND r.id = e.run_id
    '''

    lines = []

    conn = WarehouseCentralConnection()
    folder = Path(__file__).parent.absolute() / 'templates'

    with conn.query_dict(
        sql=sql__errors,
        parameters={'ts': kwargs['dag_run'].logical_date},
    ) as cursor:

        environment = Environment(loader=FileSystemLoader(folder))
        template = environment.get_template('email.j2')
        content = template.render(cursor=list(cursor)).strip()

        if len(content) > 0:
            send_email(
                html_content=content,
                to=["richard.a.bramley@uhl-tr.nhs.uk"],
                subject="Data Warehousing Errors",
            )

    logging.info("_send_email: Ended")


def create_dq_reporting(dag):
    parent_subdag = create_sub_dag_task(dag, 'dq_reporting')

    conn = WarehouseCentralConnection()

    create_run = conn.get_operator(
        task_id='INSERT__etl_run__audit',
        sql="shared_sql/INSERT__etl_run.sql",
        dag=parent_subdag.subdag,
    )

    log_dq_errors = PythonOperator(
        task_id="log_dq_errors",
        python_callable=_log_dq_errors,
        dag=parent_subdag.subdag,
    )

    send_email = PythonOperator(
        task_id="send_dq_email",
        python_callable=_send_email,
        dag=parent_subdag.subdag,
    )

    create_run >> log_dq_errors >> send_email

    return parent_subdag
