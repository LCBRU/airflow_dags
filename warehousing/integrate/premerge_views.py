import logging
from pathlib import Path
from airflow.operators.python_operator import PythonOperator
from tools import create_sub_dag_task
from warehousing.database import WarehouseCentralConnection

DWH_CONNECTION_NAME = 'DWH'


def _create_views():
    logging.info("_create_views: Started")

    conn = WarehouseCentralConnection()
    sql_dir = Path(__file__).parent.absolute() / 'sql/pre_views'
    
    for sql_file in [
        'CREATE__strip_characters.sql',
        'CREATE__study_database_name.sql',
        'CREATE_VIEW__etl__civicrm_custom.sql',
    ]:
        conn.execute(file_path=sql_dir / sql_file)

    logging.info("_create_views: Ended")


def create_wh_create_premerge_views(dag):
    parent_subdag = create_sub_dag_task(dag, 'premerge_views')

    PythonOperator(
        task_id="create_views",
        python_callable=_create_views,
        dag=parent_subdag.subdag,
    )

    return parent_subdag