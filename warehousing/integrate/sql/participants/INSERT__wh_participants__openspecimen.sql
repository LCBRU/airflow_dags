DECLARE @source_type_id INT

SELECT @source_type_id = id
FROM cfg_participant_source
WHERE name = 'OpenSpecimen'

INSERT INTO wh_participants(
    identifier,
    identifier_type_id,
    source_type_id,
    source_identifier
)
SELECT DISTINCT
	CONVERT(VARCHAR(100), op.identifier),
	cwpit.id,
	@source_type_id,
	op.identifier
FROM openspecimen__registration or2
JOIN openspecimen__participant op
	ON op.identifier = or2.participant_id
JOIN cfg_participant_identifier_type cwpit
	ON cwpit.name = 'OpenSpecimen Participant ID'
	
UNION

SELECT DISTINCT
	op.empi_id,
	cosm.participant_identifier_type_id,
	@source_type_id,
	op.identifier
FROM cfg_openspecimen_study_mapping cosm
JOIN openspecimen__registration or2 
	ON or2.collection_protocol_id = cosm.collection_protocol_id
JOIN openspecimen__participant op 
	ON op.identifier = or2.participant_id
WHERE LEN(TRIM(op.empi_id)) > 0
;