DECLARE @ts VARCHAR(100)
SELECT TOP 1 @ts=r.dag_run_ts
FROM warehouse_config.dbo.etl_run r
ORDER BY r.created_datetime DESC

DECLARE @ts_prev VARCHAR(100)
SELECT @ts_prev=r.dag_run_ts
FROM warehouse_config.dbo.etl_run r
ORDER BY r.created_datetime DESC
OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY


SELECT
	dea.group_id,
	dea_prev.count AS expected_participant_count,
	dea.count AS actual_participant_count
FROM warehouse_config.dbo.desc__etl_audit dea
LEFT JOIN warehouse_config.dbo.desc__etl_audit dea_prev
	ON dea_prev.group_id = dea.group_id 
	AND dea_prev.dag_run_ts = @ts_prev
	AND dea_prev.count_type_id  = dea.count_type_id
	AND dea_prev.group_type_id = dea.group_type_id 
	AND dea_prev.table_id  = dea.table_id
	AND dea_prev.database_id = dea.database_id
WHERE dea.dag_run_ts = @ts
	AND dea.count_type_name = 'REDCap Participant'
	AND dea.group_type_name = 'REDCap Project'
	AND dea.table_name = 'desc__redcap_data'
	AND dea.database_name = 'warehouse_central'

;
