CREATE TABLE openspecimen__specimen_group (
    identifier INT NOT NULL UNIQUE,
    name VARCHAR(500),
    comments VARCHAR(2000),
    registration_id INT REFERENCES openspecimen__registration(identifier),
    event_id INT REFERENCES openspecimen__event(identifier),
    collection_status VARCHAR(50),
    collection_comments VARCHAR(500),
    collection_timestamp DATETIME2,
    received_comments VARCHAR(500),
    received_timestamp DATETIME2,
    activity_status VARCHAR(50)
)

INSERT INTO openspecimen__specimen_group (identifier, name, comments, registration_id, event_id, collection_status, collection_comments, collection_timestamp, received_comments, received_timestamp, activity_status)
SELECT
    sg.identifier,
    sg.name,
    sg.comments,
    sg.collection_protocol_reg_id,
    sg.collection_protocol_event_id,
    sg.collection_status,
    sg.collection_comments,
    sg.collection_timestamp,
    sg.received_comments,
    sg.received_timestamp,
    sg.activity_status
FROM datalake_openspecimen.dbo.catissue_specimen_coll_group sg
;
