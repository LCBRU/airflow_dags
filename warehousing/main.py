import os
from datetime import timedelta, datetime
from airflow import DAG
from warehousing.crf_manager_download import create_download_crf_manager_studies
from warehousing.download_to_mysql import create_download_to_mysql_dag
from warehousing.edge_download import create_download_edge_studies
from warehousing.mysql_to_datalake import create_datalake_mysql_import_dag


default_args = {
    "owner": "airflow",
    "reties": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2020, 1, 1),
	'email': os.environ.get('ERROR_EMAIL_ADDRESS', '').split(';'),
	'email_on_failure': True,
}

dag = DAG(
    dag_id="load_warehouse",
    schedule_interval=os.environ.get('SCHEDULE_LOAD_WAREHOUSE', None) or None,
    default_args=default_args,
    catchup=False,
)

datalake_mysql_import = create_datalake_mysql_import_dag(dag)
download_to_mysql = create_download_to_mysql_dag(dag)
download_edge_studies = create_download_edge_studies(dag)
download_crfm_studies = create_download_crf_manager_studies(dag)

download_to_mysql >> datalake_mysql_import
download_edge_studies >> download_crfm_studies
download_edge_studies >> datalake_mysql_import
download_crfm_studies >> datalake_mysql_import
