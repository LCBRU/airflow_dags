from datetime import timedelta, datetime
from airflow import DAG
from warehousing.download_to_mysql import create_download_to_mysql_dag
from warehousing.mysql_to_datalake import create_datalake_mysql_import_dag


default_args = {
    "owner": "airflow",
    "reties": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2020, 1, 1)
}


dag = DAG(
    dag_id="load_warehouse",
    schedule_interval="0 18 * * *",
    default_args=default_args,
    catchup=False,
)

datalake_mysql_import = create_datalake_mysql_import_dag(dag)
download_to_mysql = create_download_to_mysql_dag(dag)

download_to_mysql >> datalake_mysql_import