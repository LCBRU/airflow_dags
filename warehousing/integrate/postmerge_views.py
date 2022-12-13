import logging
from pathlib import Path
from airflow.operators.python_operator import PythonOperator
from tools import create_sub_dag_task
from warehousing.database import WarehouseCentralConnection


def _create_views():
    logging.info("_create_views: Started")

    conn = WarehouseCentralConnection()
    sql_dir = Path(__file__).parent.absolute() / 'sql/post_views'
    
    for sql_file in [
        'CREATE_VIEW__desc__redcap_field.sql',
        'CREATE_VIEW__desc__redcap_log.sql',
        'CREATE_VIEW__desc__redcap_data.sql',
        'CREATE_VIEW__desc__openspecimen.sql',
    ]:
        conn.execute_mssql(file_path=sql_dir / sql_file)

    logging.info("_create_views: Ended")


def create_wh_create_postmerge_views(dag):
    parent_subdag = create_sub_dag_task(dag, 'postmerge_views')

    PythonOperator(
        task_id="create_views",
        python_callable=_create_views,
        dag=parent_subdag.subdag,
    )

    return parent_subdag