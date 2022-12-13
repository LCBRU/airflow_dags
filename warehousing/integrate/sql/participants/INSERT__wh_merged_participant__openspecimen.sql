DECLARE @cfg_participant_source_id INT

SELECT @cfg_participant_source_id = id
FROM warehouse_config.dbo.cfg_participant_source
WHERE name = 'OpenSpecimen'

INSERT INTO wh_merged_participant(
    identifier,
    cfg_participant_identifier_type_id,
    cfg_participant_source_id,
    source_identifier
)
SELECT DISTINCT
	CONVERT(VARCHAR(100), op.identifier),
	cwpit.id,
	@cfg_participant_source_id,
	op.identifier
FROM openspecimen__registration or2
JOIN openspecimen__participant op
	ON op.identifier = or2.participant_id
JOIN warehouse_config.dbo.cfg_participant_identifier_type cwpit
	ON cwpit.name = 'OpenSpecimen Participant ID'
	
UNION

SELECT DISTINCT
	op.empi_id,
	cosm.cfg_participant_identifier_type_id,
	@cfg_participant_source_id,
	op.identifier
FROM warehouse_config.dbo.cfg_openspecimen_study_mapping cosm
JOIN openspecimen__registration or2 
	ON or2.collection_protocol_id = cosm.collection_protocol_id
JOIN openspecimen__participant op 
	ON op.identifier = or2.participant_id
WHERE LEN(TRIM(op.empi_id)) > 0
;