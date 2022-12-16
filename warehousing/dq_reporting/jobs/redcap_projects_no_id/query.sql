SELECT
	cri.datalake_database,
	cri.id AS cfg_redcap_instance_id,
	mrp.redcap_project_id,
	mrp.name AS project_name
FROM meta__redcap_project mrp
JOIN warehouse_config.dbo.cfg_redcap_instance cri
	ON cri.id = mrp.cfg_redcap_instance_id
LEFT JOIN warehouse_config.dbo.cfg_redcap_identifier_field crif
	ON crif.cfg_redcap_instance_id = mrp.cfg_redcap_instance_id
	AND crif.redcap_project_id = mrp.redcap_project_id 
WHERE crif.id IS NULL