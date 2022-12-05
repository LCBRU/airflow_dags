from itertools import groupby
import logging
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator
from tools import create_sub_dag_task, query_mssql, execute_mssql, query_mssql_dict

DWH_CONNECTION_NAME = 'DWH'


def _copy_redcap():
    logging.info("_copy_civicrm: Started")

    sql__redcap_mappings = '''
        SELECT
            rpm.study_database,
            mrp.id AS meta__redcap_project_id
        FROM etl__redcap_project_mapping rpm
        JOIN meta__redcap_project mrp
            ON mrp.meta__redcap_instance_id = rpm.meta__redcap_instance_id
            AND mrp.redcap_project_id = rpm.redcap_project_id
        ORDER BY rpm.study_database
        ;
    '''

    sql__create_redcap_data = '''
        CREATE VIEW desc__redcap_data AS
        SELECT DISTINCT rd.*
        FROM warehouse_central.dbo.desc__redcap_data rd
        WHERE rd.meta__redcap_project_id IN ({meta__redcap_project_ids})
    '''

    sql__create_redcap_log = '''
        CREATE VIEW desc__redcap_log AS
        SELECT DISTINCT rl.*
        FROM warehouse_central.dbo.desc__redcap_log rl
        WHERE rl.meta__redcap_project_id IN ({meta__redcap_project_ids})
    '''

    with query_mssql_dict(DWH_CONNECTION_NAME, schema='warehouse_central', sql=sql__redcap_mappings) as cursor:
        for study_db, fields in groupby(cursor, lambda r: r['study_database']):

            meta__redcap_project_ids = [str(f['meta__redcap_project_id']) for f in fields]

            logging.info(f'****************************** {study_db}')

            execute_mssql(
                DWH_CONNECTION_NAME,
                schema=study_db,
                sql=sql__create_redcap_data.format(meta__redcap_project_ids=', '.join(meta__redcap_project_ids)),
            )

            execute_mssql(
                DWH_CONNECTION_NAME,
                schema=study_db,
                sql=sql__create_redcap_log.format(meta__redcap_project_ids=', '.join(meta__redcap_project_ids)),
            )

    logging.info("_copy_civicrm: Ended")


def _copy_openspecimen():
    logging.info("_copy_openspecimen: Started")

    sql__os_mappings = '''
        SELECT	study_database, collection_protocol_id
        FROM etl__openspecimen_mapping
        ORDER BY study_database 
    '''

    sql__create_view = '''
        CREATE VIEW desc__openspecimen AS
        SELECT os.*
        FROM warehouse_central.dbo.desc__openspecimen os
        WHERE collection_protocol_identifier IN ({collection_protocol_ids})
    '''

    with query_mssql_dict(DWH_CONNECTION_NAME, schema='warehouse_central', sql=sql__os_mappings) as cursor:
        for study_db, fields in groupby(cursor, lambda r: r['study_database']):

            collection_protocol_ids = [str(f['collection_protocol_id']) for f in fields]

            logging.info(f'****************************** {study_db}')

            execute_mssql(
                DWH_CONNECTION_NAME,
                schema=study_db,
                sql=sql__create_view.format(collection_protocol_ids=', '.join(collection_protocol_ids)),
            )

    logging.info("_copy_openspecimen: Ended")


def _copy_civicrm():
    logging.info("_copy_civicrm: Started")

    sql__civicrm_mappings = '''
        SELECT study_database, case_type_id
        FROM etl__civicrm_mapping
        ORDER BY study_database
    '''

    sql__create_case = '''
        CREATE VIEW civicrm_case AS
        SELECT cc.*
        FROM warehouse_central.dbo.civicrm__case cc
        WHERE cc.case_type_id IN ({case_type_ids})
    '''

    sql__create_contact = '''
        CREATE VIEW civicrm_contact AS
        SELECT con.*
        FROM warehouse_central.dbo.civicrm__contact con
        WHERE con.id IN (
            SELECT cc.contact_id
            FROM warehouse_central.dbo.civicrm__case cc
            WHERE cc.case_type_id IN ({case_type_ids})
        )
    '''

    with query_mssql_dict(DWH_CONNECTION_NAME, schema='warehouse_central', sql=sql__civicrm_mappings) as cursor:
        for study_db, fields in groupby(cursor, lambda r: r['study_database']):

            case_type_ids = [str(f['case_type_id']) for f in fields]

            logging.info(f'****************************** {study_db}')

            execute_mssql(
                DWH_CONNECTION_NAME,
                schema=study_db,
                sql=sql__create_case.format(case_type_ids=', '.join(case_type_ids)),
            )

            execute_mssql(
                DWH_CONNECTION_NAME,
                schema=study_db,
                sql=sql__create_contact.format(case_type_ids=', '.join(case_type_ids)),
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