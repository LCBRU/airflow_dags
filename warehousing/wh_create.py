import logging
from tools import create_sub_dag_task
from airflow.operators.mssql_operator import MsSqlOperator
from warehousing.wh_central_audit import create_wh_central_audit
from warehousing.wh_central_cleanup import create_wh_central_cleanup
from warehousing.wh_central_config import create_wh_central_config
from warehousing.wh_central_merge_civicrm_data import create_wh_central_merge_civicrm_data_dag
from warehousing.wh_central_merge_openspecimen_data import create_wh_central_merge_openspecimen_data_dag
from warehousing.wh_central_merge_participants import create_wh_central_merge_participants
from warehousing.wh_create_studies import create_wh_create_studies
from warehousing.wh_create_postmerge_views import create_wh_create_postmerge_views
from warehousing.wh_create_premerge_views import create_wh_create_premerge_views
from warehousing.wh_central_merge_redcap_data import create_wh_central_merge_redcap_data_dag


def merge_data(dag):
    parent_subdag = create_sub_dag_task(dag, 'merge_data', run_on_failures=True)

    merge_redcap_data = create_wh_central_merge_redcap_data_dag(parent_subdag.subdag)
    merge_openspecimen_data = create_wh_central_merge_openspecimen_data_dag(parent_subdag.subdag)
    merge_civicrm_data = create_wh_central_merge_civicrm_data_dag(parent_subdag.subdag)

    return parent_subdag
    

DWH_CONNECTION_NAME = 'DWH'


def _create_update_statistics(dag):
    logging.info("_create_update_statistics: Started")

    update_statistics = MsSqlOperator(
        task_id='UPDATE_STATISTICS',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="sql/wh_central_merge_data/UPDATE_STATISTICS.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    logging.info("_create_update_statistics: Ended")

    return update_statistics


def create_warehouse(dag):
    parent_subdag = create_sub_dag_task(dag, 'warehouse', run_on_failures=True)

    wh_create_cleanup = create_wh_central_cleanup(parent_subdag.subdag)
    wh_create_config = create_wh_central_config(parent_subdag.subdag)
    wh_create_premerge_views = create_wh_create_premerge_views(parent_subdag.subdag)
    wh_central_merge_data = merge_data(parent_subdag.subdag)
    wh_central_merge_participants = create_wh_central_merge_participants(parent_subdag.subdag)
    wh_create_postmerge_views = create_wh_create_postmerge_views(parent_subdag.subdag)
    wh_create_studies = create_wh_create_studies(parent_subdag.subdag)
    update_statistics = _create_update_statistics(parent_subdag.subdag)
    email_audit = create_wh_central_audit(parent_subdag.subdag)

    wh_create_cleanup >> wh_create_config >> wh_create_premerge_views >> wh_central_merge_data >> wh_central_merge_participants >> wh_create_postmerge_views >> wh_create_studies >> update_statistics >> email_audit

    return parent_subdag
