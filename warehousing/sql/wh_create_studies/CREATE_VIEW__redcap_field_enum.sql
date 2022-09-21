CREATE VIEW meta__redcap_field_enum AS
SELECT DISTINCT rfe.*
FROM warehouse_central.dbo.datalake_redcap_project_mappings m
JOIN warehouse_central.dbo.meta__redcap_instance ri
	ON ri.datalake_database = m.datalake_database
JOIN warehouse_central.dbo.meta__redcap_project rp
	ON rp.meta__instance_id = ri.id
	AND rp.redcap_project_id = m.redcap_project_id
JOIN warehouse_central.dbo.meta__redcap_form rf
	ON rf.meta__project_id = rp.id
JOIN warehouse_central.dbo.meta__redcap_form_section rfs
	ON rfs.meta__form_id = rf.id
JOIN warehouse_central.dbo.meta__redcap_field rfd
	ON rfd.meta__form_section_id = rfs.id
JOIN warehouse_central.dbo.meta__redcap_field_enum rfe
	ON rfe.meta__redcap_field_id = rfd.id
WHERE m.study_name = %(study_name)s
;