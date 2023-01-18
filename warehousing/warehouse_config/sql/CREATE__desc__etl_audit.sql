CREATE OR ALTER VIEW desc__etl_audit AS
SELECT
	ea.id,
	ea.run_id,
	er.dag_run_ts,
	er.created_datetime,
	ea.group_id,
	ea.group_type_id,
	group_type.name AS group_type_name,
	ea.cfg_participant_source_id,
	cps.name AS participant_source_name,
	ea.cfg_study_id,
	cs.name AS study_name,
	warehouse_central.dbo.study_database_name(cs.name) AS study_database_name,
	ea.database_id,
	ead.name AS database_name,
	ea.table_id,
	eat.name AS table_name,
	ea.count_type_id,
	count_type.name AS count_type_name,
	ea.count
FROM etl_audit ea
JOIN etl_run er
	ON er.id = ea.run_id
JOIN etl_audit_group_type group_type
	ON group_type.id = ea.group_type_id
LEFT JOIN cfg_participant_source cps
	ON cps.id = ea.cfg_participant_source_id
LEFT JOIN cfg_study cs
	ON cs.id = ea.cfg_study_id
JOIN etl_audit_database ead
	ON ead.id = ea.database_id
JOIN etl_audit_table eat
	ON eat.id = ea.table_id
JOIN etl_audit_group_type count_type
	ON count_type.id = ea.count_type_id
;