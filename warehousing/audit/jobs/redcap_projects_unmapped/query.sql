SELECT crp.*
FROM merged__redcap_project crp
LEFT JOIN warehouse_config.dbo.cfg_redcap_instance cri
	ON cri.datalake_database = crp.datalake_database
LEFT JOIN warehouse_config.dbo.cfg_redcap_mapping crm
	ON crm.redcap_project_id = crp.project_id
WHERE crm.id IS NULL
	AND crp.status = 1
--    -- 0 = Development
--    -- 1 = Online
--    -- 2 = ?
--    -- 3 = Archived
;