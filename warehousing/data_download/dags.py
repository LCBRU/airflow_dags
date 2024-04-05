import yaml
import pathlib
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from warehousing.data_download.crf_manager_download import download_crf_manager_studies
from warehousing.data_download.download_to_mysql import download_mysql_backup_and_restore
from warehousing.data_download.edge_download import download_edge_studies
from tools import default_dag_args
from airflow.utils.trigger_rule import TriggerRule


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


with DAG(
    dag_id="download_external_data",
    default_args=default_dag_args,
    schedule=None,
):
        task_download_edge_studies = PythonOperator(
            task_id=f"download_edge_studies",
            python_callable=download_edge_studies,
        )
        task_download_crfm_studies = PythonOperator(
            task_id=f"download_crf_manager_studies",
            python_callable=download_crf_manager_studies,
            trigger_rule=TriggerRule.ALL_DONE,
        )

        task_download_edge_studies >> task_download_crfm_studies
