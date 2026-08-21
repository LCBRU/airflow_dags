import os
from datetime import datetime

error_emails = [
    e.strip()
    for e in os.environ.get("ERROR_EMAIL_ADDRESS", "").split(";")
    if e.strip()
]

default_dag_args = {
    "owner": "airflow",
    'email': error_emails,
    'email_on_failure': True,
    "start_date": datetime(2020, 1, 1),
}


def create_sub_dag_task(dag, sub_task_id, run_on_failures=False):
    """Compatibility helper for the removed SubDagOperator API.

    Airflow 3 removed the subdag pattern. Use TaskGroups in DAG definitions instead.
    Keeping this helper as a lightweight wrapper maintains import compatibility for any
    callers that still reference it, while avoiding legacy SubDagOperator usage.
    """
    from airflow.utils.task_group import TaskGroup

    if run_on_failures:
        # SubDagOperator's trigger_rule semantics must be reimplemented with explicit
        # task relationships in the calling DAG. There is no direct TaskGroup equivalent.
        pass

    return TaskGroup(group_id=sub_task_id, dag=dag)
