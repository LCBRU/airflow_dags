CREATE TABLE openspecimen__specimen (
    identifier INT NOT NULL UNIQUE,
    label VARCHAR(500) INDEX ix__openspecimen__specimen__label,
    barcode VARCHAR(500),
    comments VARCHAR(MAX),
    specimen_group_id INT INDEX ix__openspecimen__specimen__specimen_group_id REFERENCES openspecimen__specimen_group(identifier),
    collection_protocol_id INT INDEX ix__openspecimen__specimen__collection_protocol_id REFERENCES openspecimen__collection_protocol(identifier),
    available_quantity NUMERIC(16,6),
    initial_quantity NUMERIC(16,6),
    created_on DATETIME2,
    activity_status VARCHAR(50),
    collection_status VARCHAR(50),
    specimen_class VARCHAR(50),
    specimen_type VARCHAR(50),
    lineage VARCHAR(50),
    parent_specimen_id INT REFERENCES openspecimen__specimen(identifier)
)

INSERT INTO openspecimen__specimen (identifier, label, barcode, comments, specimen_group_id, collection_protocol_id, available_quantity, initial_quantity, created_on, activity_status, collection_status, specimen_class, specimen_type, lineage, parent_specimen_id)
SELECT
    s.identifier,
    s.label,
    s.barcode,
    s.comments,
    s.specimen_collection_group_id,
    s.collection_protocol_id,
    s.available_quantity,
    s.initial_quantity,
    s.created_on,
    s.activity_status,
    s.collection_status,
    s.specimen_class,
    s.specimen_type,
    s.lineage,
    s.parent_specimen_id
FROM datalake_openspecimen.dbo.catissue_specimen s
JOIN warehouse_central.dbo.openspecimen__specimen_group sg
    ON sg.identifier = s.specimen_collection_group_id
;
