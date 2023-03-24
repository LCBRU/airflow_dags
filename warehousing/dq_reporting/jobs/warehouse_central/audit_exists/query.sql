SELECT *
FROM (
	SELECT 'record' AS count_type_name,
		'CiviCRM Case Type' AS group_type_name,
		'civicrm__case' AS table_name,
		'warehouse_central' AS database_name
	UNION SELECT 'OpenSpecimen Participant' AS count_type_name,
		'OpenSpecimen Collection Protocol' AS group_type_name,
		'desc__openspecimen' AS table_name,
		'warehouse_central' AS database_name
	UNION SELECT 'record' AS count_type_name,
		'OpenSpecimen Collection Protocol' AS group_type_name,
		'desc__openspecimen' AS table_name,
		'warehouse_central' AS database_name
	UNION SELECT 'REDCap Participant' AS count_type_name,
		'REDCap Project' AS group_type_name,
		'desc__redcap_data' AS table_name,
		'warehouse_central' AS database_name
	UNION SELECT 'record' AS count_type_name,
		'REDCap Project' AS group_type_name,
		'desc__redcap_data' AS table_name,
		'warehouse_central' AS database_name
	UNION SELECT 'record' AS count_type_name,
		'REDCap Project' AS group_type_name,
		'desc__redcap_log' AS table_name,
		'warehouse_central' AS database_name
	UNION SELECT 'record' AS count_type_name,
		'REDCap Project' AS group_type_name,
		'desc__redcap_field' AS table_name,
		'warehouse_central' AS database_name
) t
WHERE NOT EXISTS (
	SELECT 1
	FROM warehouse_config.dbo.desc__etl_audit dea
	JOIN warehouse_central.dbo.etl__last_ts ts
		ON ts.last_ts = dea.dag_run_ts
	WHERE t.count_type_name = dea.count_type_name
		AND t.group_type_name = dea.group_type_name
		AND t.table_name = dea.table_name
		AND t.database_name = dea.database_name
)


