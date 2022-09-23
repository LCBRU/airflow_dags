SET QUOTED_IDENTIFIER OFF;

CREATE TABLE meta__redcap_form_section (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    meta__redcap_form_id INT NOT NULL,
    name_index NVARCHAR(500) NOT NULL,
    name NVARCHAR(MAX) NOT NULL,
    INDEX idx__redcap_form_section__name (name_index),
    FOREIGN KEY (meta__redcap_form_id) REFERENCES meta__redcap_form(id),
    UNIQUE (meta__redcap_form_id, name_index)
);

EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'datalake_redcap_%'
BEGIN
	INSERT INTO warehouse_central.dbo.meta__redcap_form_section (meta__redcap_form_id, name_index, name)
    SELECT
        rf.id,
        LEFT(rfs.element_preceding_header, 500),
        rfs.element_preceding_header
    FROM (
        SELECT DISTINCT
            '?' datalake_database,
            project_id,
            form_name,
            element_preceding_header
        FROM [?].dbo.redcap_metadata
        WHERE LEN(TRIM(COALESCE(element_preceding_header, ''))) > 0
    ) rfs
    JOIN warehouse_central.dbo.meta__redcap_instance ri
        ON ri.datalake_database = rfs.datalake_database
    JOIN warehouse_central.dbo.meta__redcap_project rp
        ON  rp.meta__redcap_instance_id = ri.id
        AND rp.redcap_project_id = rfs.project_id
    JOIN warehouse_central.dbo.meta__redcap_form rf
        ON rf.name = rfs.form_name
        AND rf.meta__redcap_project_id = rp.id
END"

SET QUOTED_IDENTIFIER ON;
