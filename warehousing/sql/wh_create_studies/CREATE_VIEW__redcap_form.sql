CREATE VIEW meta__redcap_form AS
SELECT DISTINCT rf.*
FROM warehouse_central.dbo.datalake_redcap_project_mappings m
JOIN warehouse_central.dbo.meta__redcap_instance ri
	ON ri.datalake_database = m.datalake_database
JOIN warehouse_central.dbo.meta__redcap_project rp
	ON rp.meta__redcap_instance_id = ri.id
	AND rp.redcap_project_id = m.redcap_project_id
JOIN warehouse_central.dbo.meta__redcap_form rf
	ON rf.meta__redcap_project_id = rp.id
WHERE m.study_name = %(study_name)s
;
