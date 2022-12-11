import logging
from tools import create_sub_dag_task
from airflow.operators.mssql_operator import MsSqlOperator
from warehousing.integrate.wh_central_cleanup import create_wh_central_cleanup
from warehousing.integrate.wh_central_merge_civicrm_data import create_wh_central_merge_civicrm_data_dag
from warehousing.integrate.wh_central_merge_openspecimen_data import create_wh_central_merge_openspecimen_data_dag
from warehousing.integrate.wh_central_merge_participants import create_wh_central_merge_participants
from warehousing.integrate.wh_create_postmerge_views import create_wh_create_postmerge_views
from warehousing.integrate.wh_create_premerge_views import create_wh_create_premerge_views
from warehousing.integrate.wh_central_merge_redcap_data import create_wh_central_merge_redcap_data_dag


def _create_merge_data(dag):
    parent_subdag = create_sub_dag_task(dag, 'merge_data', run_on_failures=True)

    create_wh_central_merge_redcap_data_dag(parent_subdag.subdag)
    create_wh_central_merge_openspecimen_data_dag(parent_subdag.subdag)
    create_wh_central_merge_civicrm_data_dag(parent_subdag.subdag)

    return parent_subdag
    

DWH_CONNECTION_NAME = 'DWH'


def _create_update_statistics(dag):
    logging.info("_create_update_statistics: Started")

    update_statistics = MsSqlOperator(
        task_id='UPDATE_STATISTICS',
        mssql_conn_id=DWH_CONNECTION_NAME,
        sql="integrate/sql/UPDATE_STATISTICS.sql",
        autocommit=True,
        database='warehouse_central',
        dag=dag,
    )

    logging.info("_create_update_statistics: Ended")

    return update_statistics


def create_warehouse(dag):
    parent_subdag = create_sub_dag_task(dag, 'warehouse', run_on_failures=True)

    cleanup = create_wh_central_cleanup(parent_subdag.subdag)
    premerge_views = create_wh_create_premerge_views(parent_subdag.subdag)
    merge_data = _create_merge_data(parent_subdag.subdag)
    merge_participants = create_wh_central_merge_participants(parent_subdag.subdag)
    postmerge_views = create_wh_create_postmerge_views(parent_subdag.subdag)
    update_statistics = _create_update_statistics(parent_subdag.subdag)

    cleanup >> premerge_views >> merge_data >> merge_participants >> postmerge_views >> update_statistics

    return parent_subdag
