import logging
from pathlib import Path
from tools import create_sub_dag_task
from warehousing.database import WarehouseCentralConnection
from airflow.operators.python_operator import PythonOperator


def _drop_old_data():
    logging.info("_drop_old_data: Started")

    sql_dir = Path(__file__).parent.absolute() / 'sql/cleanup'
    conn = WarehouseCentralConnection()

    for sql_file in [
        'DROP__Participants.sql',
        'DROP__Civicrm.sql',
        'DROP__OpenSpecimen.sql',
        'DROP__meta_redcap_tables.sql',
    ]:
        conn.execute_mssql(file_path=sql_dir / sql_file)

    logging.info("_drop_old_data: Ended")


def create_drop_old_data(dag):
    return PythonOperator(
        task_id="drop_old_data",
        python_callable=_drop_old_data,
        dag=dag,
    )
