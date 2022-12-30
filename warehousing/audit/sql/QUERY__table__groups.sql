IF NOT EXISTS(SELECT 1 FROM warehouse_config.dbo.etl_audit_database WHERE name = DB_NAME())
BEGIN
    INSERT INTO warehouse_config.dbo.etl_audit_database (name)
    VALUES (DB_NAME());
END

IF NOT EXISTS(SELECT 1 FROM warehouse_config.dbo.etl_audit_table WHERE name = '{table_name}')
BEGIN
    INSERT INTO warehouse_config.dbo.etl_audit_table (name)
    VALUES ('{table_name}');
END

IF OBJECT_ID('{table_name}') IS NOT NULL
BEGIN
	INSERT INTO warehouse_config.dbo.etl_audit (run_id, group_id, group_type_id, cfg_participant_source_id, cfg_study_id, database_id, table_id, count_type_id, count)
	SELECT
		er.id,
		{group_id_term} AS group_id,
		gt.id AS group_type_id,
		cps.id AS cfg_participant_source_id,
		NULL AS cfg_study_id,
		ad.id AS database_id,
		atab.id AS table_id,
		ct.id AS count_type_id,
		COUNT({count_term}) records
	FROM {table_name} t
	JOIN warehouse_config.dbo.etl_run er
		ON er.dag_run_ts = '{ts}'
	JOIN warehouse_config.dbo.etl_audit_database ad
		ON ad.name = DB_NAME()
	JOIN warehouse_config.dbo.etl_audit_table atab
		ON atab.name = '{table_name}'
	JOIN warehouse_config.dbo.cfg_participant_source cps
		ON cps.name = '{participant_source}'
	JOIN warehouse_config.dbo.etl_audit_group_type gt
		ON gt.name = '{group_type}'
	JOIN warehouse_config.dbo.etl_audit_group_type ct
		ON ct.name = '{count_type}'
	GROUP BY
		cps.id,
		gt.id,
		ct.id,
		{group_id_term},
		er.id,
		ad.id,
		atab.id
END

