CREATE VIEW meta__redcap_data AS
SELECT DISTINCT rv.*
FROM warehouse_central.dbo.redcap_data rv
JOIN warehouse_central.dbo.meta__redcap_instance mri 
	ON mri.id = rv.meta__redcap_instance_id
JOIN warehouse_central.dbo.etl__redcap_project_mapping m
	ON mri.datalake_database = m.datalake_database
WHERE m.study_name = %(study_name)s
;