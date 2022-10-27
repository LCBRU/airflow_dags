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
;
