import logging
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator
from tools import create_sub_dag_task, query_mssql, execute_mssql

DWH_CONNECTION_NAME = 'DWH'


def _create_redcap_views():
    logging.info("_create_study_databases: Started")

    study_details = []

    with query_mssql(DWH_CONNECTION_NAME, schema='warehouse_central', file_path='wh_create_studies/QUERY__studies.sql') as cursor:
        study_details = [(study_db, study_name) for (study_db, study_name) in cursor]

    for study_db, study_name in study_details:
        logging.info(f'****************************** {study_name}: {study_db}')

        for sql_file in [
            'CREATE_VIEW__redcap_instance.sql',
            'CREATE_VIEW__redcap_project.sql',
            'CREATE_VIEW__redcap_form.sql',
            'CREATE_VIEW__redcap_form_section.sql',
            'CREATE_VIEW__redcap_field.sql',
            'CREATE_VIEW__redcap_field_enum.sql',
            'CREATE_VIEW__redcap_value.sql',
        ]:
            execute_mssql(
                DWH_CONNECTION_NAME,
                schema=study_db,
                file_path=f'wh_create_studies/{sql_file}',
                parameters={'study_name': study_name},
            )

    logging.info("_create_study_databases: Ended")


def _create_study_whs(dag):
    logging.info("_create_study_whs: Started")

    create_study_db_name = MsSqlOperator(
        task_id='create_study_db_name',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_create_studies/CREATE__study_database_name.sql",
        autocommit=True,
        dag=dag,
        database='warehouse_central',
    )

    create_study_wh_dbs = MsSqlOperator(
        task_id='create_study_wh_dbs',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_create_studies/CREATE__warehouse_databases.sql",
        autocommit=True,
        dag=dag,
        database='warehouse_central',
    )

    create_redcap_views = PythonOperator(
        task_id="create_redcap_views",
        python_callable=_create_redcap_views,
        dag=dag,
    )

    create_study_db_name >> create_study_wh_dbs
    create_study_wh_dbs >> create_redcap_views

    logging.info("_create_study_whs: Ended")


def create_wh_create_studies(dag):
    parent_subdag = create_sub_dag_task(dag, 'wh_create_studies')

    _create_study_whs(dag=parent_subdag.subdag)

    return parent_subdag