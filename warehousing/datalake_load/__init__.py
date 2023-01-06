import logging
from pathlib import Path
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator
from itertools import groupby
from tools import create_sub_dag_task
from warehousing.database import WarehouseConnection, WarehouseCentralConnection


DWH_CONNECTION_NAME = 'DWH'


def _create_indexes_procedure(destination_database, source_database):
    logging.info("_create_indexes_procedure: Started")

    create_index_template = '''
        CREATE INDEX {index_name}
        ON {table_name} ({columns});
    '''

    collations = {
        'A': '',
        'D': 'DESCENDING',
    }

    sql_updated = '''
        SELECT name
        FROM _etl_tables
        WHERE extant = 1
            AND exclude = 0
            AND (last_copied IS NULL OR last_copied < last_updated)
        ;
    '''

    conn_dest = WarehouseConnection(destination_database)

    with conn_dest.query(sql=sql_updated) as cursor:
        updated = [r[0] for r in cursor]

    indexes = []

    conn = WarehouseCentralConnection()

    with conn.query(
        file_path=Path(__file__).parent.absolute() / 'sql/QUERY__source_index_details.sql',
        parameters={'source_database': source_database},
        ) as cursor:

        for (table_name, index_name), fields in groupby(cursor, key=lambda x: (x[0], x[1])):

            if table_name not in updated:
                continue

            columns = []
            contains_text_field = False

            for (_, _, fieldname, collation, data_type, max_length) in fields:
                if data_type in ['text', 'tinytext', 'mediumtext', 'longtext']:
                    contains_text_field = True

                columns.append(f"[{fieldname}] {collations[collation or 'A']}")

            if not contains_text_field:
                indexes.append(create_index_template.format(
                    index_name=f'[{index_name}]',
                    table_name=f'[{table_name}]',
                    columns=', '.join(columns),
                ))

    sql = '\n\n'.join(indexes)

    conn_dest.execute(sql=sql)

    logging.info("_create_indexes_procedure: Ended")


def _create_database_copy_dag(dag, source_database, destination_database):
    logging.info("_create_database_copy_dag: Started")

    create_destination_database = MsSqlOperator(
        task_id='create_destination_database',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="datalake_load/sql/CREATE__databases.sql",
        autocommit=True,
        dag=dag,
        parameters={'db_name': destination_database},
    )

    create_etl_tables = MsSqlOperator(
        task_id='CREATE__etl_tables',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="datalake_load/sql/CREATE__etl_tables.sql",
        autocommit=True,
        database=destination_database,
        dag=dag,
    )

    recreate_etl_tables = MsSqlOperator(
        task_id='recreate_etl_tables',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="datalake_load/sql/INSERT__etl_tables.sql",
        autocommit=True,
        database=destination_database,
        dag=dag,
        parameters={'source_database': source_database},
    )

    copy_tables = MsSqlOperator(
        task_id='copy_tables',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="datalake_load/sql/INSERT__tables.sql",
        autocommit=True,
        database=destination_database,
        dag=dag,
        parameters={'source_database': source_database},
    )

    change_text_columns_to_varchar = MsSqlOperator(
        task_id='change_text_columns_to_varchar',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="datalake_load/sql/UPDATE__tables__alter_text_to_varchar.sql",
        autocommit=True,
        database=destination_database,
        dag=dag,
        parameters={},
    )

    create_indexes = PythonOperator(
        task_id="create_indexes",
        python_callable=_create_indexes_procedure,
        dag=dag,
        op_kwargs={
            'destination_database': destination_database,
            'source_database': source_database,
        },
    )

    mark_updated = MsSqlOperator(
        task_id='mark_updated',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="datalake_load/sql/UPDATE__etl_tables__last_copied.sql",
        autocommit=True,
        database=destination_database,
        dag=dag,
        parameters={'source_database': source_database},
    )

    (
        create_destination_database >>
        create_etl_tables >>
        recreate_etl_tables >>
        copy_tables >>
        change_text_columns_to_varchar >>
        create_indexes >>
        mark_updated
    )

    logging.info("_create_database_copy_dag: Ended")


details = {
    'civicrmlive_docker4716': 'datalake_civicrm',
    'drupallive_docker4716': 'datalake_civicrm_drupal',
    'identity': 'datalake_identity',
    'briccs_northampton': 'datalake_onyx_northampton',
    'briccs': 'datalake_onyx_uhl',
    'uol_openspecimen': 'datalake_openspecimen',
    'uol_easyas_redcap': 'datalake_redcap_easyas',
    'redcap_genvasc': 'datalake_redcap_genvasc',
    'uol_survey_redcap': 'datalake_redcap_internet',
    'redcap6170_briccsext': 'datalake_redcap_n3',
    'redcap_national': 'datalake_redcap_national',
    'redcap6170_briccs': 'datalake_redcap_uhl',
    'uol_crf_redcap': 'datalake_redcap_uol',
}


def create_datalake_mysql_import_dag(dag):
    parent_subdag = create_sub_dag_task(dag, 'datalake_mysql_import', run_on_failures=True)

    for source, destination in details.items():
        subdag = create_sub_dag_task(parent_subdag.subdag, f'{source}__to__{destination}')

        _create_database_copy_dag(
            dag=subdag.subdag,
            source_database=source,
            destination_database=destination,
        )

    return parent_subdag