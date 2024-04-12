from datetime import datetime
import os
import logging
from pathlib import Path
from airflow import DAG
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator
from itertools import groupby
from warehousing.database import MsSqlConnection
from tools import default_dag_args


def _create_indexes_procedure(connection_name, destination_database, source_database):
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

    conn_dest = MsSqlConnection(connection_name, destination_database)

    with conn_dest.query(sql=sql_updated) as cursor:
        updated = [r[0] for r in cursor]

    indexes = []

    conn = MsSqlConnection(connection_name, 'master')

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


def _create_database_copy_dag(connection_name, source_database, destination_database):
    logging.info("_create_database_copy_dag: Started")

    task_id_suffix = f'__{connection_name}_{destination_database}'

    create_destination_database = MsSqlOperator(
        task_id=f'create_destination_database{task_id_suffix}',
        mssql_conn_id=connection_name,
        sql="CREATE__databases.sql",
        autocommit=True,
        parameters={'db_name': destination_database},
    )

    create_etl_tables = MsSqlOperator(
        task_id=f'CREATE__etl_tables{task_id_suffix}',
        mssql_conn_id=connection_name,
        sql="CREATE__etl_tables.sql",
        autocommit=True,
        database=destination_database,
    )

    recreate_etl_tables = MsSqlOperator(
        task_id=f'recreate_etl_tables{task_id_suffix}',
        mssql_conn_id=connection_name,
        sql="INSERT__etl_tables.sql",
        autocommit=True,
        database=destination_database,
        parameters={'source_database': source_database},
    )

    copy_tables = MsSqlOperator(
        task_id=f'copy_tables{task_id_suffix}',
        mssql_conn_id=connection_name,
        sql="INSERT__tables.sql",
        autocommit=True,
        database=destination_database,
        parameters={'source_database': source_database},
    )

    change_text_columns_to_varchar = MsSqlOperator(
        task_id=f'change_text_columns_to_varchar{task_id_suffix}',
        mssql_conn_id=connection_name,
        sql="UPDATE__tables__alter_text_to_varchar.sql",
        autocommit=True,
        database=destination_database,
        parameters={},
    )

    create_indexes = PythonOperator(
        task_id=f"create_indexes{task_id_suffix}",
        python_callable=_create_indexes_procedure,
        op_kwargs={
            'destination_database': destination_database,
            'source_database': source_database,
            'connection_name': connection_name,
        },
    )

    mark_updated = MsSqlOperator(
        task_id=f'mark_updated{task_id_suffix}',
        mssql_conn_id=connection_name,
        sql="UPDATE__etl_tables__last_copied.sql",
        autocommit=True,
        database=destination_database,
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


with DAG(
    dag_id="Copy_live_DB_to_DWH",
    default_args=default_dag_args,
    schedule=os.environ.get('SCHEDULE_DATALAKE_LOAD', None) or None,
    template_searchpath = ['/opt/airflow/dags/warehousing/datalake_load/sql/'],
    start_date=datetime(2020, 1, 1),
    catchup=False,
):
    # Legacy DWH
    _create_database_copy_dag(connection_name='LEGACY_DWH', source_database='civicrmlive_docker4716', destination_database='datalake_civicrmlive_docker4716')
    _create_database_copy_dag(connection_name='LEGACY_DWH', source_database='drupallive_docker4716', destination_database='datalake_drupallive_docker4716')
    _create_database_copy_dag(connection_name='LEGACY_DWH', source_database='identity', destination_database='datalake_identity')
    _create_database_copy_dag(connection_name='LEGACY_DWH', source_database='briccs_northampton', destination_database='datalake_briccs_northampton')
    _create_database_copy_dag(connection_name='LEGACY_DWH', source_database='briccs', destination_database='datalake_briccs')
    _create_database_copy_dag(connection_name='LEGACY_DWH', source_database='uol_openspecimen', destination_database='datalake_openspecimen')
    _create_database_copy_dag(connection_name='LEGACY_DWH', source_database='uol_easyas_redcap', destination_database='datalake_redcap_easyas')
    _create_database_copy_dag(connection_name='LEGACY_DWH', source_database='redcap_genvasc', destination_database='datalake_redcap_genvasc')
    _create_database_copy_dag(connection_name='LEGACY_DWH', source_database='uol_survey_redcap', destination_database='datalake_uol_survey_redcap')
    _create_database_copy_dag(connection_name='LEGACY_DWH', source_database='redcap6170_briccsext', destination_database='datalake_redcap6170_briccsext')
    _create_database_copy_dag(connection_name='LEGACY_DWH', source_database='redcap_national', destination_database='datalake_redcap_national')
    _create_database_copy_dag(connection_name='LEGACY_DWH', source_database='redcap6170_briccs', destination_database='datalake_redcap6170_briccs')
    _create_database_copy_dag(connection_name='LEGACY_DWH', source_database='uol_crf_redcap', destination_database='datalake_uol_crf_redcap')

    # # New DWH
    # _create_database_copy_dag(connection_name='DWH', source_database='civicrmlive_docker4716', destination_database='datalake_civicrm')
    # _create_database_copy_dag(connection_name='DWH', source_database='drupallive_docker4716', destination_database='datalake_civicrm_drupal')
    # _create_database_copy_dag(connection_name='DWH', source_database='identity', destination_database='datalake_identity')
    # _create_database_copy_dag(connection_name='DWH', source_database='briccs_northampton', destination_database='datalake_onyx_northampton')
    # _create_database_copy_dag(connection_name='DWH', source_database='briccs', destination_database='datalake_onyx_uhl')
    # _create_database_copy_dag(connection_name='DWH', source_database='uol_openspecimen', destination_database='datalake_openspecimen')
    # _create_database_copy_dag(connection_name='DWH', source_database='uol_easyas_redcap', destination_database='datalake_redcap_easyas')
    # _create_database_copy_dag(connection_name='DWH', source_database='redcap_genvasc', destination_database='datalake_redcap_genvasc')
    # _create_database_copy_dag(connection_name='DWH', source_database='uol_survey_redcap', destination_database='datalake_redcap_internet')
    # _create_database_copy_dag(connection_name='DWH', source_database='redcap6170_briccsext', destination_database='datalake_redcap_n3')
    # _create_database_copy_dag(connection_name='DWH', source_database='redcap_national', destination_database='datalake_redcap_national')
    # _create_database_copy_dag(connection_name='DWH', source_database='redcap6170_briccs', destination_database='datalake_redcap_uhl')
    # _create_database_copy_dag(connection_name='DWH', source_database='uol_crf_redcap', destination_database='datalake_redcap_uol')
