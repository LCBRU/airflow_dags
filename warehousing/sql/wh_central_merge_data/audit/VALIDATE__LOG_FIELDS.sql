SELECT COUNT(*)
FROM (
	SELECT DISTINCT mri.datalake_database, mrp.name project_name, mrp.redcap_project_id, rl.field_name 
	FROM warehouse_central.dbo.redcap_log rl 
	JOIN meta__redcap_event mre 
		ON mre.id = rl.meta__redcap_event_id 
	JOIN meta__redcap_arm mra 
		ON mra.id = mre.meta__redcap_arm_id 
	JOIN meta__redcap_project mrp 
		ON mrp.id = mra.meta__redcap_project_id 
	JOIN meta__redcap_instance mri 
		ON mri.id = mrp.meta__redcap_instance_id
	JOIN combined_redcap_metadata crm 
		ON crm.datalake_database = mri.datalake_database 
		AND crm.project_id = mrp.redcap_project_id 
		AND crm.field_name = rl.field_name 
	WHERE rl.meta__redcap_field_id IS NULL
		AND crm.datalake_database IS NULL
) x
;
