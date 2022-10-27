CREATE TABLE openspecimen__event (
    identifier INT NOT NULL UNIQUE,
    collection_point_label VARCHAR(500),
    collection_protocol_id INT REFERENCES openspecimen__collection_protocol(identifier),
    activity_status VARCHAR(50)
)

INSERT INTO openspecimen__event (identifier, collection_point_label, collection_protocol_id, activity_status)
SELECT
    e.identifier,
    e.collection_point_label,
    e.collection_protocol_id,
    e.activity_status
FROM datalake_openspecimen.dbo.catissue_coll_prot_event e
JOIN warehouse_central.dbo.openspecimen__collection_protocol cp
    ON cp.identifier = e.collection_protocol_id
;
