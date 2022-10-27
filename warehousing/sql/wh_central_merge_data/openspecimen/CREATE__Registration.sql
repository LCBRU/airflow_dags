CREATE TABLE openspecimen__registration (
    identifier INT NOT NULL UNIQUE,
    participant_id INT REFERENCES openspecimen__participant(identifier),
    collection_protocol_id INT REFERENCES openspecimen__collection_protocol(identifier),
    protocol_participant_id NVARCHAR(500),
    registration_date DATE,
    activity_status VARCHAR(50)
)

INSERT INTO openspecimen__registration (identifier, participant_id, collection_protocol_id, protocol_participant_id, registration_date, activity_status)
SELECT
    r.identifier,
    r.participant_id,
    r.collection_protocol_id,
    r.protocol_participant_id,
    r.registration_date,
    r.activity_status
FROM datalake_openspecimen.dbo.catissue_coll_prot_reg r
JOIN warehouse_central.dbo.openspecimen__collection_protocol cp
    ON cp.identifier = r.collection_protocol_id
;
