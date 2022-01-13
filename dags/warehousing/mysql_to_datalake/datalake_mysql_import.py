import os
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag import SubDagOperator
from itertools import groupby


DWH_CONNECTION_NAME = 'DWH'
CUR_DIR = os.path.abspath(os.path.dirname(__file__))


def _query_mssql(connection_name, sql, parameters=None):
    if not parameters:
        parameters = {}

    mysql = MsSqlHook(mssql_conn_id=connection_name)
    conn = mysql.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql, parameters)
    return cursor


def _ddl_mssql(connection_name, sql):
    mysql = MsSqlHook(mssql_conn_id=connection_name)
    conn = mysql.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()


def create_indexes_procedure(destination_database, source_database):
    create_index_template = '''
        CREATE INDEX {index_name}
        ON {table_name} ({columns});
    '''

    collations = {
        'A': '',
        'D': 'DESCENDING',
    }

    with open(f'{CUR_DIR}/sql/get_index_details.sql') as sql:
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

    print(sql)

    _ddl_mssql(DWH_CONNECTION_NAME, sql)


def create_database_copy_dag(parent_dag_id, sub_task_id, source_database, destination_database, args):
    result = DAG(
        dag_id=f"{parent_dag_id}.{sub_task_id}",
        default_args=args,
    )

    create_etl_tables = MsSqlOperator(
        task_id='CREATE__etl_tables',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/CREATE__etl_tables.sql",
        autocommit=True,
        database=destination_database,
        dag=result,
    )

    recreate_etl_tables = MsSqlOperator(
        task_id='recreate_etl_tables',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/recreate_etl_tables.sql",
        autocommit=True,
        database=destination_database,
        dag=result,
        parameters={'source_database': source_database},
    )

    copy_tables = MsSqlOperator(
        task_id='copy_tables',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/copy_tables.sql",
        autocommit=True,
        database=destination_database,
        dag=result,
        parameters={'source_database': source_database},
    )

    create_indexes = PythonOperator(
        task_id="create_indexes",
        python_callable=create_indexes_procedure,
        dag=result,
        op_kwargs={
            'destination_database': destination_database,
            'source_database': source_database,
        },
    )

    mark_updated = MsSqlOperator(
        task_id='mark_updated',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/mark_updated.sql",
        autocommit=True,
        database=destination_database,
        dag=result,
        parameters={'source_database': source_database},
    )

    (
        create_etl_tables >>
        recreate_etl_tables >>
        copy_tables >>
        create_indexes >>
        mark_updated
    )

    return result

default_args = {
    "owner": "airflow",
    "reties": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2020, 1, 1)
}

def create_dag_task(dag, source_database, destination_database):
    task_id = f"{source_database}__to__{destination_database}"

    SubDagOperator(
        task_id=task_id,
        subdag=create_database_copy_dag(
            parent_dag_id=dag.dag_id,
            sub_task_id=task_id,
            source_database=source_database,
            destination_database=destination_database,
            args=dag.default_args
        ),
        default_args=dag.default_args,
        dag=dag,
    )


dag = DAG(
    dag_id="datalake_mysql_import",
    schedule_interval="0 22 * * *",
    default_args=default_args,
    catchup=False,
)

create_dag_task(dag, 'civicrmlive_docker4716', 'datalake_civicrm')
create_dag_task(dag, 'drupallive_docker4716', 'datalake_civicrm_drupal')
create_dag_task(dag, 'identity', 'datalake_identity')
create_dag_task(dag, 'briccs_northampton', 'datalake_onyx_northampton')
create_dag_task(dag, 'briccs', 'datalake_onyx_uhl')
create_dag_task(dag, 'uol_openspecimen', 'datalake_openspecimen')
create_dag_task(dag, 'uol_easyas_redcap', 'datalake_redcap_easyas')
create_dag_task(dag, 'redcap_genvasc', 'datalake_redcap_genvasc')
create_dag_task(dag, 'uol_survey_redcap', 'datalake_redcap_internet')
create_dag_task(dag, 'redcap6170_briccsext', 'datalake_redcap_n3')
create_dag_task(dag, 'redcap_national', 'datalake_redcap_national')
create_dag_task(dag, 'redcap6170_briccs', 'datalake_redcap_uhl')
create_dag_task(dag, 'uol_crf_redcap', 'datalake_redcap_uol')
