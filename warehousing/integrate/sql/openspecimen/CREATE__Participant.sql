CREATE TABLE openspecimen__participant (
    identifier INT NOT NULL UNIQUE,
    empi_id NVARCHAR(50) INDEX ix_openspecimen__participant__empi_id,
    activity_status VARCHAR(50)
)

INSERT INTO openspecimen__participant (identifier, empi_id, activity_status)
SELECT
    p.identifier,
    p.empi_id,
    p.activity_status
FROM datalake_openspecimen.dbo.catissue_participant p
WHERE p.identifier IN (
    SELECT DISTINCT participant_id
    FROM datalake_openspecimen.dbo.catissue_coll_prot_reg r
        JOIN warehouse_central.dbo.openspecimen__collection_protocol cp
            ON cp.identifier = r.collection_protocol_id
)
;
