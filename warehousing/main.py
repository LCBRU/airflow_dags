import os
from datetime import timedelta, datetime
from airflow import DAG
from tools import create_sub_dag_task
from warehousing.crf_manager_download import create_download_crf_manager_studies
from warehousing.download_to_mysql import create_download_to_mysql_dag
from warehousing.edge_download import create_download_edge_studies
from warehousing.mysql_to_datalake import create_datalake_mysql_import_dag
from warehousing.wh_central_merge_data import create_wh_central_merge_data_dag
from warehousing.wh_create_studies import create_wh_create_studies
from warehousing.wh_create_postmerge_views import create_wh_create_postmerge_views
from warehousing.wh_create_premerge_views import create_wh_create_premerge_views


def create_download_data(dag):
    parent_subdag = create_sub_dag_task(dag, 'download_data', run_on_failures=True)

    download_to_mysql = create_download_to_mysql_dag(parent_subdag.subdag)
    download_edge_studies = create_download_edge_studies(parent_subdag.subdag)
    download_crfm_studies = create_download_crf_manager_studies(parent_subdag.subdag)

    download_edge_studies << download_crfm_studies

    return parent_subdag
    

def create_warehouse(dag):
    parent_subdag = create_sub_dag_task(dag, 'warehouse', run_on_failures=True)

    wh_create_premerge_views = create_wh_create_premerge_views(parent_subdag.subdag)
    wh_central_merge_data = create_wh_central_merge_data_dag(parent_subdag.subdag)
    wh_create_postmerge_views = create_wh_create_postmerge_views(parent_subdag.subdag)
    wh_create_studies = create_wh_create_studies(parent_subdag.subdag)

    wh_create_premerge_views >> wh_central_merge_data >> wh_create_postmerge_views >> wh_create_studies

    return parent_subdag
    

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

download_data = create_download_data(dag)
datalake_mysql_import = create_datalake_mysql_import_dag(dag)
warehouse = create_warehouse(dag)

download_data >> datalake_mysql_import >> warehouse
