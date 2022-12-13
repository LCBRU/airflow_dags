from itertools import groupby
import logging
import re

from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator

from tools import create_sub_dag_task
from warehousing.database import DatalakeCiviCRMConnection, WarehouseCentralConnection, execute_mssql, query_mssql, DWH_CONNECTION_NAME


def _create_merge_civicrm_data_dag(dag):
    logging.info("_create_merge_civicrm_data_dag: Started")

    parent_subdag = create_sub_dag_task(dag, 'merge_data', run_on_failures=True)

    create_cases = MsSqlOperator(
        task_id='CREATE__Civicrm_Case',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="integrate/sql/civicrm/CREATE__Civicrm_Case.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create_contacts = MsSqlOperator(
        task_id='CREATE__Civicrm_Contact',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="integrate/sql/civicrm/CREATE__Civicrm_Contact.sql",
        autocommit=True,
        database='warehouse_central',
        dag=parent_subdag.subdag,
    )

    create_contacts >> create_cases

    logging.info("_create_merge_civicrm_data_dag: Ended")

    return parent_subdag


def _copy_custom():
    logging.info("_copy_custom: Started")

    custom_fields = []

    conn = WarehouseCentralConnection()
    
    sql__field_details = '''
        SELECT
            ecc.table_name,
            ecc.warehouse_table_name,
            ccf.column_name AS custom_field_column_name
        FROM datalake_civicrm.dbo.civicrm_custom_field ccf
        JOIN etl__civicrm_custom ecc
            ON ecc.custom_group_id = ccf.custom_group_id
        ORDER BY ecc.warehouse_table_name, ccf.id
    '''

    with conn.query_mssql(sql=sql__field_details) as cursor:
        custom_fields = [(
            table_name,
            warehouse_table_name,
            column_name
        ) for (
            table_name,
            warehouse_table_name,
            column_name
        ) in cursor]

    cv_conn = DatalakeCiviCRMConnection()
    
    for (table_name, warehouse_table_name), g in groupby(custom_fields, lambda x: (x[0], x[1])):
        logging.info(f'****************************** {table_name}')

        res = r'_+\d+$'

        fields = []

        for _, _, custom_field_column_name in g:
            fn = re.sub(res, '', custom_field_column_name)
            fields.append(f'[{custom_field_column_name}] AS [{fn}]')

        field_list = ', '.join(fields)

        sql = f'''
            SELECT
                cc.id AS case_id,
                cc.case_type_id,
                {field_list}
            INTO warehouse_central.dbo.{warehouse_table_name}
            FROM {table_name} cv
            JOIN civicrm_case cc
                ON cc.id = cv.entity_id
        '''

        cv_conn.execute_mssql(sql=sql)

    logging.info("_copy_custom: Ended")


def create_wh_central_merge_civicrm_data_dag(dag):
    parent_subdag = create_sub_dag_task(dag, 'merge_civicrm_data')

    merge = _create_merge_civicrm_data_dag(dag=parent_subdag.subdag)

    copy_custom = PythonOperator(
        task_id="copy_custom",
        python_callable=_copy_custom,
        dag=parent_subdag.subdag,
    )

    merge >> copy_custom

    return parent_subdag
