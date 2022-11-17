from itertools import groupby
import logging
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator
from tools import create_sub_dag_task, query_mssql, execute_mssql

DWH_CONNECTION_NAME = 'DWH'


def _copy_redcap():
    logging.info("_copy_civicrm: Started")

    sql__redcap_mappings = '''
        SELECT DISTINCT study_database FROM etl__redcap_project_mapping
    '''

    with query_mssql(DWH_CONNECTION_NAME, schema='warehouse_central', sql=sql__redcap_mappings) as cursor:
        studies = [study_db for (study_db,) in cursor]

    print(studies)

    sql__create_redcap_data = '''
        CREATE VIEW desc__redcap_data AS
        SELECT DISTINCT rd.*
        FROM warehouse_central.dbo.desc__redcap_data rd
        JOIN warehouse_central.dbo.etl__redcap_project_mapping rpm
            ON rpm.meta__redcap_instance_id = rd.meta__redcap_instance_id
            AND rpm.redcap_project_id = rd.redcap_project_id
        WHERE rpm.study_database = %(study_db)s
    '''

    sql__create_redcap_log = '''
        CREATE VIEW desc__redcap_log AS
        SELECT DISTINCT rl.*
        FROM warehouse_central.dbo.desc__redcap_log rl
        JOIN warehouse_central.dbo.etl__redcap_project_mapping rpm
            ON rpm.meta__redcap_instance_id = rl.meta__redcap_instance_id
            AND rpm.redcap_project_id = rl.redcap_project_id
        WHERE rpm.study_database = %(study_db)s
    '''

    for study_db in studies:
        logging.info(f'****************************** {study_db}')

        execute_mssql(
            DWH_CONNECTION_NAME,
            schema=study_db,
            sql=sql__create_redcap_data,
            parameters={
                'study_db': study_db,
            },
        )

        execute_mssql(
            DWH_CONNECTION_NAME,
            schema=study_db,
            sql=sql__create_redcap_log,
            parameters={
                'study_db': study_db,
            },
        )

    logging.info("_copy_civicrm: Ended")


def _copy_openspecimen():
    logging.info("_copy_openspecimen: Started")

    sql__os_mappings = '''
        SELECT DISTINCT study_database
        FROM etl__openspecimen_mapping
    '''

    with query_mssql(DWH_CONNECTION_NAME, schema='warehouse_central', sql=sql__os_mappings) as cursor:
        studies = [study_db for (study_db,) in cursor]

    sql__create_view = '''
        CREATE VIEW desc__openspecimen AS
        SELECT os.*
        FROM warehouse_central.dbo.desc__openspecimen os
        JOIN warehouse_central.dbo.etl__openspecimen_mapping osm
            ON osm.collection_protocol_id = os.collection_protocol_identifier
        WHERE osm.study_database = %(study_db)s
    '''

    for study_db in studies:
        logging.info(f'****************************** {study_db}')

        execute_mssql(
            DWH_CONNECTION_NAME,
            schema=study_db,
            sql=sql__create_view,
            parameters={'study_db': study_db},
        )

    logging.info("_copy_openspecimen: Ended")


def _copy_civicrm():
    logging.info("_copy_civicrm: Started")

    sql__civicrm_mappings = '''
        SELECT DISTINCT study_database FROM etl__civicrm_mapping
    '''

    with query_mssql(DWH_CONNECTION_NAME, schema='warehouse_central', sql=sql__civicrm_mappings) as cursor:
        studies = [study_db for (study_db,) in cursor]

    sql__create_case = '''
        CREATE VIEW civicrm_case AS
        SELECT cc.*
        FROM warehouse_central.dbo.civicrm__case cc
        JOIN warehouse_central.dbo.etl__civicrm_mapping cm
            ON cm.case_type_id = cc.case_type_id
        WHERE cm.study_database = %(study_db)s
    '''

    sql__create_contact = '''
        CREATE VIEW civicrm_contact AS
        SELECT cc.*
        FROM warehouse_central.dbo.civicrm__contact cc
        WHERE cc.id IN (
            SELECT ccase.contact_id
            FROM warehouse_central.dbo.civicrm__case ccase
            JOIN warehouse_central.dbo.etl__civicrm_mapping cm
                ON cm.case_type_id = ccase.case_type_id
            WHERE cm.case_type_id = %(study_db)s
        )
    '''

    for study_db in studies:
        logging.info(f'****************************** {study_db}')

        execute_mssql(
            DWH_CONNECTION_NAME,
            schema=study_db,
            sql=sql__create_case,
            parameters={'study_db': study_db},
        )

        execute_mssql(
            DWH_CONNECTION_NAME,
            schema=study_db,
            sql=sql__create_contact,
            parameters={'study_db': study_db},
        )

    logging.info("_copy_civicrm: Ended")


def _copy_civicrm_custom():
    logging.info("_copy_civicrm_custom: Started")

    sql__custom_table_mappings = '''
        SELECT
            ecc.warehouse_table_name,
            ecm.study_database
        FROM etl__civicrm_custom ecc
        JOIN etl__civicrm_mapping ecm 
            ON ecm.case_type_id = ecc.case_type_id
    '''

    with query_mssql(DWH_CONNECTION_NAME, schema='warehouse_central', sql=sql__custom_table_mappings) as cursor:
        mappings = [(table_name, study_db) for (table_name, study_db) in cursor]

    sql__create_view = '''
        CREATE VIEW {table_name} AS
        SELECT *
        FROM warehouse_central.dbo.{table_name}
    '''

    for table_name, study_db in mappings:
        logging.info(f'****************************** {table_name} > {study_db}: {study_db}')

        execute_mssql(
            DWH_CONNECTION_NAME,
            schema=study_db,
            sql=sql__create_view.format(table_name=table_name)
        )

    logging.info("_copy_civicrm_custom: Ended")


def _create_study_whs(dag):
    logging.info("_create_study_whs: Started")

    create_study_wh_dbs = MsSqlOperator(
        task_id='create_study_wh_dbs',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_create_studies/CREATE__warehouse_databases.sql",
        autocommit=True,
        dag=dag,
        database='warehouse_central',
    )

    copy_redcap = PythonOperator(
        task_id="copy_redcap",
        python_callable=_copy_redcap,
        dag=dag,
    )

    copy_openspecimen = PythonOperator(
        task_id="copy_openspecimen",
        python_callable=_copy_openspecimen,
        dag=dag,
    )

    copy_civicrm = PythonOperator(
        task_id="copy_civicrm",
        python_callable=_copy_civicrm,
        dag=dag,
    )

    copy_civicrm_custom = PythonOperator(
        task_id="copy_custom",
        python_callable=_copy_civicrm_custom,
        dag=dag,
    )

    create_study_wh_dbs >> copy_redcap >> copy_openspecimen >> copy_civicrm >> copy_civicrm_custom

    logging.info("_create_study_whs: Ended")


def create_wh_create_studies(dag):
    parent_subdag = create_sub_dag_task(dag, 'wh_create_studies')

    _create_study_whs(dag=parent_subdag.subdag)

    return parent_subdag