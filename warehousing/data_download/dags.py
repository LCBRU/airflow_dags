from airflow.operators.python_operator import PythonOperator
from tools import create_sub_dag_task
from warehousing.data_download.edge_download import _download_edge_studies


def create_download_edge_studies(dag):
    parent_subdag = create_sub_dag_task(dag, 'download_edge_studies', run_on_failures=True)

    PythonOperator(
        task_id=f"download_edge_studies",
        python_callable=_download_edge_studies,
        dag=parent_subdag.subdag,
    )

    return parent_subdag
