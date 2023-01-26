SELECT
	ame.datalake_database,
	ame.source_system,
	ame.project_id,
	ame.project_name,
	ame.participant_count AS expected_count,
	dea.count AS actual_count
FROM warehouse_central.dbo.audit__manual_expected ame
JOIN warehouse_central.dbo.etl__civicrm_custom ecc
	ON ecc.case_type_id = ame.project_id
LEFT JOIN warehouse_config.dbo.desc__etl_audit dea
	ON dea.group_id = CONVERT(VARCHAR, ame.project_id)
	AND dea.count_type_name = 'record'
	AND dea.group_type_name = 'table'
	AND dea.table_name = ecc.table_name
	AND dea.database_name = 'warehouse_central'
JOIN etl__last_ts ts
	ON ts.last_ts = dea.dag_run_ts
WHERE ame.source_system = 'CiviCRM Case'
	AND COALESCE(ame.participant_count, 0) > COALESCE(dea.count, 0)
;
