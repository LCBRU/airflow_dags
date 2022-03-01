from pathlib import Path
from airflow import DAG
from airflow.operators.subdag import SubDagOperator


def create_sub_dag_task(dag, sub_task_id):
    subdag = DAG(
        dag_id=f"{dag.dag_id}.{sub_task_id}",
        default_args=dag.default_args,
    )

    return SubDagOperator(
        task_id=sub_task_id,
        subdag=subdag,
        default_args=dag.default_args,
        dag=dag,
    )


def sql_path():
    return Path(__file__).parent.absolute() / 'sql'
