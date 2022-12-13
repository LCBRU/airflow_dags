import logging
from pathlib import Path
from tools import create_sub_dag_task
from warehousing.database import WarehouseCentralConnection, execute_mssql
from airflow.operators.python_operator import PythonOperator


def _merge_participants():
    logging.info("_create_views: Started")

    conn = WarehouseCentralConnection()
    sql_dir = Path(__file__).parent.absolute() / 'sql'
    
    conn.execute_mssql(
        file_path=sql_dir / 'cleanup/DROP__Participants.sql',
    )

    for sql_file in [
        'CREATE__wh_merged_participant.sql',
        'INSERT__wh_merged_participant__redcap.sql',
        'INSERT__wh_merged_participant__openspecimen.sql',
        'INSERT__wh_merged_participant__table_column.sql',
        'MERGE__wh_merged_participant.sql',
    ]:
        conn.execute_mssql(file_path=sql_dir / 'participants' / sql_file)

    logging.info("_create_views: Ended")


def create_wh_central_merge_participants(dag):
    parent_subdag = create_sub_dag_task(dag, 'merge_participants')

    PythonOperator(
        task_id="merge_participants",
        python_callable=_merge_participants,
        dag=parent_subdag.subdag,
    )

    return parent_subdag
