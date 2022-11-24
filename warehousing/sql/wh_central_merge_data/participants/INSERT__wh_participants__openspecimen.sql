DECLARE @source_type_id INT

SELECT @source_type_id = id
FROM cfg_wh_participant_source
WHERE name = 'REDCap'

INSERT INTO wh_participants(
    identifier,
    identifier_type_id,
    source_type_id,
    source_identifier
)
SELECT DISTINCT
	TRIM(rd.text_value),
	crif.cfg_participant_id_type_id,
    @source_type_id,
	rd.redcap_participant_id
FROM warehouse_central.dbo.cfg_redcap_id_fields crif
JOIN warehouse_central.dbo.meta__redcap_field mrf 
	ON mrf.name = crif.field_name
JOIN warehouse_central.dbo.redcap_data rd 
	ON rd.meta__redcap_field_id = mrf.id
JOIN warehouse_central.dbo.meta__redcap_project mrp 
	ON mrp.id = rd.meta__redcap_project_id
	AND mrp.redcap_project_id = crif.project_id 
JOIN warehouse_central.dbo.meta__redcap_instance mri 
	ON mri.id = rd.meta__redcap_instance_id
	AND mri.datalake_database = crif.database_name
WHERE LEN(TRIM(rd.text_value)) > 0
