SELECT
	ame.datalake_database,
	ame.source_system,
	ame.project_id,
	ame.project_name,
	ame.participant_count AS expected_count,
	dea.count AS actual_count
FROM warehouse_central.dbo.audit__manual_expected ame
LEFT JOIN warehouse_config.dbo.desc__etl_audit dea
	ON dea.group_id = ame.project_id
	AND dea.count_type_name = 'record'
	AND dea.group_type_name = 'OpenSpecimen Collection Protocol'
	AND dea.table_name = 'desc__openspecimen'
	AND dea.database_name = 'warehouse_central'
JOIN etl__last_ts ts
	ON ts.last_ts = dea.dag_run_ts
WHERE ame.source_system = 'OpenSpecimen'
	AND COALESCE(ame.participant_count, 0) > COALESCE(dea.count, 0)
;
