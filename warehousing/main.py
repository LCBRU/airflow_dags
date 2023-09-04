import os
from datetime import timedelta, datetime
from airflow import DAG
from tools import create_sub_dag_task
from warehousing.audit import create_audit_dag
from warehousing.dq_reporting import create_dq_reporting
from warehousing.warehouse_config import create_wh_central_config
from warehousing.data_download.crf_manager_download import create_download_crf_manager_studies
from warehousing.data_download.download_to_mysql import create_download_to_mysql_dag
from warehousing.data_download.dags import create_download_edge_studies
from warehousing.datalake_load import create_datalake_mysql_import_dag, create_legacy_datalake_mysql_import_dag
from warehousing.study_warehouses import create_wh_create_studies
from warehousing.integrate import create_warehouse


def create_download_data(dag):
    parent_subdag = create_sub_dag_task(dag, 'download_data', run_on_failures=True)

    download_to_mysql = create_download_to_mysql_dag(parent_subdag.subdag)
    download_edge_studies = create_download_edge_studies(parent_subdag.subdag)
    download_crfm_studies = create_download_crf_manager_studies(parent_subdag.subdag)

    download_edge_studies << download_crfm_studies

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
legacy_datalake_mysql_import = create_legacy_datalake_mysql_import_dag(dag)
config = create_wh_central_config(dag)
warehouse = create_warehouse(dag)
create_study_warehouses = create_wh_create_studies(dag)
audit = create_audit_dag(dag)
email_dq = create_dq_reporting(dag)

download_data >> legacy_datalake_mysql_import >> datalake_mysql_import >> config >> warehouse >> create_study_warehouses >> audit >> email_dq
