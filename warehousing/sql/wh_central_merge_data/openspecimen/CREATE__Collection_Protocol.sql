CREATE TABLE openspecimen__collection_protocol (
    identifier INT NOT NULL,
    title NVARCHAR(500),
    short_title NVARCHAR(500),
    activity_status VARCHAR(50),
    UNIQUE (identifier),
    UNIQUE (title),
    UNIQUE (short_title)
)

INSERT INTO openspecimen__collection_protocol (identifier, title, short_title, activity_status)
SELECT
    cp.identifier,
    cp.title,
    cp.short_title,
    cp.activity_status
FROM datalake_openspecimen.dbo.catissue_collection_protocol cp
;
