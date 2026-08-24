from datetime import datetime
import os
from airflow import DAG
from warehousing.data_download.download_to_mysql import download_mysql_backup_and_restore
from warehousing.data_download.edge_download import download_edge_studies
from tools import default_dag_args


with DAG(
    dag_id="download_UOL_data",
    default_args=default_dag_args,
    schedule=os.environ.get('SCHEDULE_DOWNLOAD_UOL_DATA', None) or None,
    catchup=False,
    start_date=datetime(2020, 1, 1),
):
    download_mysql_backup_and_restore.override(task_id=f"download_mysql_backup_and_restore__uol_openspecimen")(
        destination_database='uol_openspecimen',
        source_url='https://catissue-live.lcbru.le.ac.uk/publish/catissue.db',
    )

    # download_mysql_backup_and_restore.override(task_id=f"download_mysql_backup_and_restore__uol_survey_redcap")(
    #     destination_database='uol_survey_redcap',
    #     source_url='https://redcap.lcbru.le.ac.uk/publish/redcap.db',
    # )


with DAG(
    dag_id="download_external_data",
    default_args=default_dag_args,
    schedule=os.environ.get('SCHEDULE_DOWNLOAD_EXTERNAL_DATA', None) or None,
    catchup=False,
    start_date=datetime(2020, 1, 1),
):
        download_mysql_backup_and_restore.override(task_id=f"download_mysql_backup_and_restore__uol_crf_redcap")(
            destination_database='uol_crf_redcap',
            source_url='https://redcap.lcbru.le.ac.uk/publish/redcap.db',
        )
