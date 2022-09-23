CREATE VIEW meta__redcap_value AS
SELECT DISTINCT rv.*
FROM warehouse_central.dbo.redcap_value rv
JOIN warehouse_central.dbo.meta__redcap_instance mri 
	ON mri.id = rv.meta__redcap_instance_id
JOIN warehouse_central.dbo.datalake_redcap_project_mappings m
	ON mri.datalake_database = m.datalake_database
WHERE m.study_name = %(study_name)s
;