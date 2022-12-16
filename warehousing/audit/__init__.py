from tools import create_sub_dag_task
from warehousing.database import WarehouseCentralConnection
from collections import namedtuple


def create_audit_dag(dag):
    parent_subdag = create_sub_dag_task(dag, 'audit')

    conn = WarehouseCentralConnection()

    conn.get_operator(
        task_id='INSERT__etl_run',
        sql="shared_sql/INSERT__etl_run.sql",
        dag=parent_subdag.subdag,
    )
    conn.get_operator(
        task_id='QUERY__CiviCRM_Custom__records',
        sql="audit/sql/QUERY__CiviCRM_Custom__records.sql",
        dag=parent_subdag.subdag,
    )

    create_table_record_counts_dag(parent_subdag.subdag)
    create_table_group_counts_dag(parent_subdag.subdag)

    return parent_subdag

def create_table_record_counts_dag(dag):
    parent_subdag = create_sub_dag_task(dag, 'table_record_counts')

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
        Params('meta__redcap_instance', 'REDCap'),
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
        conn.get_operator(
            task_id=f'QUERY__warehouse_central__{p.table_name}__records',
            sql="audit/sql/QUERY__table__records.sql",
            dag=parent_subdag.subdag,
            params=p._asdict(),
        )

    return dag


def create_table_group_counts_dag(dag):
    parent_subdag = create_sub_dag_task(dag, 'table_group_counts')

    conn = WarehouseCentralConnection()

    Params = namedtuple(
        'Params',
        'table_name participant_source group_id_term group_type count_type'
    )

    for p in [
        Params('civicrm__case', 'CiviCRM Case', 'case_type_id', 'CiviCRM Case Type', 'record'),
        Params('openspecimen__registration', 'OpenSpecimen', 'collection_protocol_id', 'OpenSpecimen Collection Protocol', 'record'),
        Params('openspecimen__specimen', 'OpenSpecimen', 'collection_protocol_id', 'OpenSpecimen Collection Protocol', 'record'),
        Params('desc__redcap_data', 'REDCap', 'datalake_database + \'-\' + CONVERT(VARCHAR(100), redcap_project_id)', 'REDCap Project', 'record'),
        Params('desc__redcap_log', 'REDCap', 'datalake_database + \'-\' + CONVERT(VARCHAR(100), redcap_project_id)', 'REDCap Project', 'record'),
        Params('desc__redcap_field', 'REDCap', 'datalake_database + \'-\' + CONVERT(VARCHAR(100), redcap_project_id)', 'REDCap Project', 'record'),
    ]:
        conn.get_operator(
            task_id=f'QUERY__warehouse_central__{p.table_name}__group__{p.group_type}',
            sql="audit/sql/QUERY__table__groups.sql",
            dag=parent_subdag.subdag,
            params=p._asdict(),
        )

    return dag
