SELECT
	crm.cfg_study_id,
	crm.cfg_redcap_instance_id,
	crm.redcap_project_id 
FROM warehouse_config.dbo.cfg_redcap_mapping crm
WHERE NOT EXISTS (
	SELECT 1
	FROM warehouse_central.dbo.meta__redcap_project mrp
	WHERE mrp.cfg_redcap_instance_id = crm.cfg_redcap_instance_id
		AND mrp.redcap_project_id = crm.redcap_project_id 
)
;
