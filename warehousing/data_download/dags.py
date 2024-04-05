import yaml
import pathlib
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from tools import create_sub_dag_task
from warehousing.data_download.download_to_mysql import download_mysql_backup_and_restore
from warehousing.data_download.edge_download import _download_edge_studies
from tools import default_dag_args


def create_download_edge_studies(dag):
    parent_subdag = create_sub_dag_task(dag, 'download_edge_studies', run_on_failures=True)

    PythonOperator(
        task_id=f"download_edge_studies",
        python_callable=_download_edge_studies,
        dag=parent_subdag.subdag,
    )

    return parent_subdag


with DAG(
    dag_id="download_UOL_data",
    default_args=default_dag_args,
    schedule=None,
):
    for conf_file in (pathlib.Path(__file__).parent.absolute() / 'uol_data_sources').glob('*.yml'):
        with conf_file.open() as f:
            conf = yaml.safe_load(f)

            if not conf.get('skip', False):
                PythonOperator(
                    task_id=f"download_mysql_backup_and_restore__{conf['destination']}",
                    python_callable=download_mysql_backup_and_restore,
                    op_kwargs={
                        'destination_database': conf['destination'],
                        'source_url': conf['sourcel_url'],
                    },
                )
