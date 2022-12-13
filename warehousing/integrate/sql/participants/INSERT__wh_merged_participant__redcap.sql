DECLARE @cfg_participant_source_id INT

SELECT @cfg_participant_source_id = id
FROM warehouse_config.dbo.cfg_participant_source
WHERE name = 'REDCap'

INSERT INTO wh_merged_participant(
    identifier,
    cfg_participant_identifier_type_id,
    cfg_participant_source_id,
    source_identifier
)
SELECT DISTINCT
	TRIM(rd.text_value),
	crif.cfg_participant_identifier_type_id,
    @cfg_participant_source_id,
	rd.redcap_participant_id
FROM warehouse_config.dbo.cfg_redcap_identifier_field crif
JOIN warehouse_central.dbo.meta__redcap_field mrf 
	ON mrf.name = crif.field_name
JOIN warehouse_central.dbo.redcap_data rd 
	ON rd.meta__redcap_field_id = mrf.id
	AND rd.cfg_redcap_instance_id = crif.cfg_redcap_instance_id
JOIN warehouse_central.dbo.meta__redcap_project mrp 
	ON mrp.id = rd.meta__redcap_project_id
	AND mrp.redcap_project_id = crif.redcap_project_id
	AND mrp.cfg_redcap_instance_id = crif.cfg_redcap_instance_id
WHERE LEN(TRIM(rd.text_value)) > 0
