import logging
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python_operator import PythonOperator
from itertools import groupby
from warehousing.tools import create_sub_dag_task, sql_path

DWH_CONNECTION_NAME = 'DWH'


def _query_mssql(connection_name, sql, parameters=None):
    logging.info("_query_mssql: Started")

    if not parameters:
        parameters = {}

    mysql = MsSqlHook(mssql_conn_id=connection_name)
    conn = mysql.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql, parameters)

    logging.info("_query_mssql: Ended")

    return cursor


def _ddl_mssql(connection_name, sql):
    logging.info("_ddl_mssql: Started")

    mysql = MsSqlHook(mssql_conn_id=connection_name)
    conn = mysql.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()

    logging.info("_ddl_mssql: Ended")


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

    with open(sql_path() / 'mysql_to_datalake/QUERY__source_index_details.sql') as sql:
        cursor =  _query_mssql(
            DWH_CONNECTION_NAME,
            sql.read(),
            parameters={'source_database': source_database},
        )

    indexes = []

    updated = [r[0] for r in _query_mssql(
        DWH_CONNECTION_NAME,
        f'''
        SELECT name
        FROM {destination_database}.dbo._etl_tables
        WHERE extant = 1
            AND exclude = 0
            AND (last_copied IS NULL OR last_copied < last_updated)
        ;''',
    ).fetchall()]

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

    sql = f'USE {destination_database};\n\n{sql}'

    _ddl_mssql(DWH_CONNECTION_NAME, sql)

    logging.info("_create_indexes_procedure: Ended")


def _create_database_copy_dag(dag, source_database, destination_database):
    logging.info("_create_database_copy_dag: Started")

    create_etl_tables = MsSqlOperator(
        task_id='CREATE__etl_tables',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/mysql_to_datalake/CREATE__etl_tables.sql",
        autocommit=True,
        database=destination_database,
        dag=dag,
    )

    recreate_etl_tables = MsSqlOperator(
        task_id='recreate_etl_tables',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/mysql_to_datalake/INSERT__etl_tables.sql",
        autocommit=True,
        database=destination_database,
        dag=dag,
        parameters={'source_database': source_database},
    )

    copy_tables = MsSqlOperator(
        task_id='copy_tables',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/mysql_to_datalake/INSERT__tables.sql",
        autocommit=True,
        database=destination_database,
        dag=dag,
        parameters={'source_database': source_database},
    )

    change_text_columns_to_varchar = MsSqlOperator(
        task_id='change_text_columns_to_varchar',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/mysql_to_datalake/UPDATE__tables__alter_text_to_varchar.sql",
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
        sql="sql/mysql_to_datalake/UPDATE__etl_tables__last_copied.sql",
        autocommit=True,
        database=destination_database,
        dag=dag,
        parameters={'source_database': source_database},
    )

    (
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