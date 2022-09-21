import os
from datetime import timedelta, datetime
from airflow import DAG
from extracts.easy_as_severe_screening import create_easy_as_severe_screening_dag


default_args = {
    "owner": "airflow",
    "reties": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2020, 1, 1),
	'email_on_failure': True,
}

dag = DAG(
    dag_id="exports",
    schedule_interval=os.environ.get('SCHEDULE_LOAD_WAREHOUSE', None) or None,
    default_args=default_args,
    catchup=False,
)

easy_as_severe_screening = create_easy_as_severe_screening_dag(dag)
