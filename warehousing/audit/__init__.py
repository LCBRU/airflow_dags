from itertools import groupby, tee
from pathlib import Path
from tools import create_sub_dag_task
from warehousing.database import WarehouseCentralConnection, WarehouseConfigConnection, WarehouseConnection, SCH_WAREHOUSE_CENTRAL
from collections import namedtuple
from airflow.operators.python_operator import PythonOperator


def create_audit_dag(dag):
    parent_subdag = create_sub_dag_task(dag, 'audit')

    conn = WarehouseCentralConnection()

    create_id = conn.get_operator(
        task_id='INSERT__etl_run__audit',
        sql="shared_sql/INSERT__etl_run.sql",
        dag=parent_subdag.subdag,
    )
    wc_record_counts = PythonOperator(
        task_id=f"warehouse_central_record_counts",
        python_callable=_table_record_counts,
        dag=parent_subdag.subdag,
    )
    wc_group_count = PythonOperator(
        task_id=f"warehouse_central_group_counts",
        python_callable=_table_group_count,
        dag=parent_subdag.subdag,
        op_kwargs={
            'study_id': '',
            'db_name': SCH_WAREHOUSE_CENTRAL,
        },
    )
    wc_civi_custom_group_count = PythonOperator(
        task_id=f"warehouse_central_civi_custom_group_count",
        python_callable=_civi_custom_group_count,
        dag=parent_subdag.subdag,
        op_kwargs={
            'db_name': SCH_WAREHOUSE_CENTRAL,
        },
    )


    wc_civi_custom = create_civicrm_custom_record_count_dag(parent_subdag.subdag, conn)
    study_record_counts = create_study_table_record_count_dags(parent_subdag.subdag)

    create_id >> wc_record_counts >> wc_group_count >> wc_civi_custom >> study_record_counts >> wc_civi_custom_group_count
    create_id >> wc_civi_custom_group_count

    return parent_subdag


def _table_record_counts(**kwargs):
    conn = WarehouseCentralConnection()

    Params = namedtuple(
        'Params',
        'table_name participant_source'
    )

    for p in [
        Params('civicrm__case', 'CiviCRM Case'),
        Params('civicrm__contact', 'CiviCRM Contact'),
        Params('meta__redcap_arm', 'REDCap'),
        Params('meta__redcap_data_type', 'REDCap'),
        Params('meta__redcap_event', 'REDCap'),
        Params('meta__redcap_field', 'REDCap'),
        Params('meta__redcap_field_enum', 'REDCap'),
        Params('meta__redcap_form', 'REDCap'),
        Params('meta__redcap_form_section', 'REDCap'),
        Params('meta__redcap_project', 'REDCap'),
        Params('openspecimen__collection_protocol', 'OpenSpecimen'),
        Params('openspecimen__event', 'OpenSpecimen'),
        Params('openspecimen__nanodrop', 'OpenSpecimen'),
        Params('openspecimen__participant', 'OpenSpecimen'),
        Params('openspecimen__registration', 'OpenSpecimen'),
        Params('openspecimen__specimen', 'OpenSpecimen'),
        Params('openspecimen__specimen_group', 'OpenSpecimen'),
        Params('redcap_data', 'REDCap'),
        Params('redcap_file', 'REDCap'),
        Params('redcap_log', 'REDCap'),
        Params('redcap_participant', 'REDCap'),
    ]:
        conn.execute(
            file_path=Path(__file__).parent.absolute() / "sql/QUERY__table__records_hook.sql",
            context={**p._asdict(), **kwargs},
        )


def create_civicrm_custom_record_count_dag(dag, conn):
    parent_subdag = create_sub_dag_task(dag, 'civicrm_custom_record_count')

    wh_conn = WarehouseCentralConnection()

    run_id = wh_conn.get_operator(
        task_id='INSERT__etl_run__civicrm_custom_record_count',
        sql="shared_sql/INSERT__etl_run.sql",
        dag=parent_subdag.subdag,
    )

    sql__custom_civicrm = '''
        SELECT * FROM etl__civicrm_custom;
    '''
    with wh_conn.query_dict(sql=sql__custom_civicrm) as cursor:
        for t in cursor:
            print(f'****** {t["warehouse_table_name"]}')
            job = conn.get_operator(
                task_id=f'QUERY__warehouse_central__{t["warehouse_table_name"]}__records',
                sql="audit/sql/QUERY__table__records.sql",
                dag=parent_subdag.subdag,
                params={
                    'table_name': t['warehouse_table_name'],
                    'participant_source': 'CiviCRM Case',
                },
            )

            run_id >> job

    return parent_subdag


def create_study_table_record_count_dags(dag):
    parent_subdag = create_sub_dag_task(dag, 'study_table_record_count')

    conf_conn = WarehouseConfigConnection()

    run_id = conf_conn.get_operator(
        task_id=f'INSERT__etl_run__study_table_record_count',
        sql="shared_sql/INSERT__etl_run.sql",
        dag=parent_subdag.subdag,
    )

    sql__study_databases = '''
        SELECT
            id,
            warehouse_central.dbo.study_database_name(name) AS db_name
        FROM cfg_study
        ORDER BY id;
    '''

    with conf_conn.query_dict(sql=sql__study_databases) as cursor:
        for study in cursor:
            study_group_counts = PythonOperator(
                task_id=f"study_group_counts__{study['db_name']}",
                python_callable=_table_group_count,
                dag=parent_subdag.subdag,
                op_kwargs={
                    'study_id': study['id'],
                    'db_name': study['db_name'],
                },
            )

            run_id >> study_group_counts
    
    return parent_subdag


def _table_group_count(study_id, db_name, **kwargs):
    redcap_group = 'datalake_database + \'-\' + CONVERT(VARCHAR(100), redcap_project_id)'

    conn = WarehouseConnection(schema=db_name)

    Params = namedtuple(
        'Params',
        'table_name participant_source group_id_term group_type count_type count_term'
    )

    for p in [
        Params(
            'civicrm__case',
            'CiviCRM Case',
            'case_type_id',
            'CiviCRM Case Type',
            'record',
            '*',
        ),
        Params(
            'desc__openspecimen',
            'OpenSpecimen',
            'collection_protocol_identifier',
            'OpenSpecimen Collection Protocol',
            'record',
            '*',
        ),
        Params(
            'desc__openspecimen',
            'OpenSpecimen',
            'collection_protocol_identifier',
            'OpenSpecimen Collection Protocol',
            'OpenSpecimen Participant',
            'participant_identifier',
        ),
        Params(
            'desc__redcap_data',
            'REDCap',
            redcap_group,
            'REDCap Project',
            'record',
            '*',
        ),
        Params(
            'desc__redcap_data',
            'REDCap',
            redcap_group,
            'REDCap Project',
            'REDCap Participant',
            'DISTINCT redcap_participant_id',
        ),
        Params(
            'desc__redcap_log',
            'REDCap',
            redcap_group,
            'REDCap Project',
            'record',
            '*',
        ),
        Params(
            'desc__redcap_field',
            'REDCap',
            redcap_group,
            'REDCap Project',
            'record',
            '*',
        ),
    ]:
        hook = conn.execute(
            file_path=Path(__file__).parent.absolute() / "sql/QUERY__table__groups.sql",
            context={**p._asdict(), **kwargs},
        )


def _civi_custom_group_count(db_name, **kwargs):
    conn = WarehouseConnection(schema=db_name)

    with conn.query_dict(sql='SELECT warehouse_table_name FROM etl__civicrm_custom;') as cursor:
        for t in cursor:
            hook = conn.execute(
                file_path=Path(__file__).parent.absolute() / "sql/QUERY__table__groups.sql",
                context={
                    'table_name': t["warehouse_table_name"],
                    'participant_source': 'CiviCRM Case',
                    'group_id_term': f'\'{t["warehouse_table_name"]}-\' + CONVERT(VARCHAR, case_type_id)',
                    'group_type': 'CiviCRM Case Type',
                    'count_type': 'record',
                    'count_term': '*',
                    **kwargs,
                },
            )
