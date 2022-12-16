import logging
from pathlib import Path
from tools import create_sub_dag_task
from warehousing.database import WarehouseConfigConnection, DWH_CONNECTION_NAME
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator


def _create_config():
    logging.info("_create_config: Started")

    conn = WarehouseConfigConnection()

    sql_dir = Path(__file__).parent.absolute() / 'sql'

    for sql_file in [
        'CREATE__cfg_study.sql',
        'CREATE__etl_audit_group_type.sql',
        'CREATE__etl_run.sql',
        'CREATE__etl_audit_database.sql',
        'CREATE__etl_audit_table.sql',
        'CREATE__etl_audit.sql',
        'CREATE__cfg_redcap_instance.sql',
        'CREATE__cfg_redcap_mapping.sql',
        'CREATE__etl_error.sql',
        'CREATE__cfg_participant_identifier_type.sql',
        'CREATE__cfg_participant_source.sql',
        'CREATE__cfg_participant_identifier_table_column.sql',
        'CREATE__cfg_redcap_identifier_field.sql',
        'CREATE__cfg_openspecimen_study_mapping.sql',
        'CREATE__cfg_civicrm_study_mapping.sql',
    ]:
        conn.execute_mssql(file_path= sql_dir / sql_file)

    logging.info("_create_config: Ended")


def create_wh_central_config(dag):
    parent_subdag = create_sub_dag_task(dag, 'initialise_config')

    conn = WarehouseConfigConnection()

    create_config_database = conn.get_operator(
        task_id='create_warehouse_config',
        sql="warehouse_config/sql/CREATE_DB__warehouse_config.sql",
        dag=parent_subdag.subdag,
    )

    create_config = PythonOperator(
        task_id="create_config",
        python_callable=_create_config,
        dag=parent_subdag.subdag,
    )

    create_run = conn.get_operator(
        task_id='INSERT__etl_run',
        sql="shared_sql/INSERT__etl_run.sql",
        dag=parent_subdag.subdag,
    )

    create_config_database >> create_config >> create_run

    return parent_subdag
