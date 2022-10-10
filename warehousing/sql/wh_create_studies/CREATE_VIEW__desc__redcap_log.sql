CREATE VIEW desc__redcap_log AS
SELECT DISTINCT rv.*
FROM warehouse_central.dbo.desc__redcap_log rv
JOIN warehouse_central.dbo.meta__redcap_event mre
	ON mre.id = rv.meta__redcap_event_id
JOIN warehouse_central.dbo.meta__redcap_arm mra
	ON mra.id = mre.meta__redcap_arm_id
JOIN warehouse_central.dbo.meta__redcap_project mrp
	ON mrp.id = mra.meta__redcap_project_id
JOIN warehouse_central.dbo.meta__redcap_instance mri 
	ON mri.id = mrp.meta__redcap_instance_id
JOIN warehouse_central.dbo.etl__redcap_project_mapping m
	ON mri.datalake_database = m.datalake_database
WHERE m.study_name = %(study_name)s
;