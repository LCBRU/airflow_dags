DECLARE @ts VARCHAR(100)
SELECT TOP 1 @ts=r.dag_run_ts
FROM warehouse_config.dbo.etl_run r
ORDER BY r.created_datetime DESC

SELECT
	ame.datalake_database,
	ame.source_system,
	ame.project_id,
	ame.project_name,
	ame.participant_count AS expected_participant_count,
	dea.count AS actual_participant_count
FROM warehouse_central.dbo.audit__manual_expected ame
LEFT JOIN warehouse_config.dbo.desc__etl_audit dea
	ON dea.group_id = ame.datalake_database + '-' + CONVERT(VARCHAR, ame.project_id)
	AND dea.dag_run_ts = @ts
	AND dea.count_type_name = 'REDCap Participant'
	AND dea.group_type_name = 'REDCap Project'
	AND dea.table_name = 'desc__redcap_data'
	AND dea.database_name = 'warehouse_central'
WHERE ame.source_system = 'redcap'
	AND COALESCE(ame.participant_count, 0) > COALESCE(dea.count, 0)
;
