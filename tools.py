import os
from airflow import DAG
from airflow.operators.subdag import SubDagOperator
from datetime import timedelta, datetime


def create_dag(title, schedule_name):
    return DAG(
        dag_id=title,
        schedule_interval=os.environ.get(schedule_name, None) or None,
        default_args={
            "owner": "airflow",
            "reties": 3,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2020, 1, 1),
            'email': os.environ.get('ERROR_EMAIL_ADDRESS', '').split(';'),
            'email_on_failure': False,
        },
        catchup=False,
    )



def create_sub_dag_task(dag, sub_task_id, run_on_failures=False):
    subdag = DAG(
        dag_id=f"{dag.dag_id}.{sub_task_id}",
        default_args=dag.default_args,
    )

    params = {}

    if run_on_failures:
        params['trigger_rule'] = 'all_done'

    return SubDagOperator(
        task_id=sub_task_id,
        subdag=subdag,
        default_args=dag.default_args,
        dag=dag,
        **params,
    )
