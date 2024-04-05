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
    PythonOperator(
        task_id=f"download_mysql_backup_and_restore__uol_openspecimen",
        python_callable=download_mysql_backup_and_restore,
        op_kwargs={
            'destination_database': 'uol_openspecimen',
            'source_url': 'https://catissue-live.lcbru.le.ac.uk/publish/catissue.db',
        },
    )
    
    PythonOperator(
        task_id=f"download_mysql_backup_and_restore__uol_easyas_redcap",
        python_callable=download_mysql_backup_and_restore,
        op_kwargs={
            'destination_database': 'uol_easyas_redcap',
            'source_url': 'https://easy-as.lbrc.le.ac.uk/publish/redcap.db',
        },
    )
    
    PythonOperator(
        task_id=f"download_mysql_backup_and_restore__uol_survey_redcap",
        python_callable=download_mysql_backup_and_restore,
        op_kwargs={
            'destination_database': 'uol_survey_redcap',
            'source_url': 'https://redcap.lcbru.le.ac.uk/publish/redcap.db',
        },
    )
    
    PythonOperator(
        task_id=f"download_mysql_backup_and_restore__uol_crf_redcap",
        python_callable=download_mysql_backup_and_restore,
        op_kwargs={
            'destination_database': 'uol_crf_redcap',
            'source_url': 'https://crf.lcbru.le.ac.uk/publish/redcap.db',
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
