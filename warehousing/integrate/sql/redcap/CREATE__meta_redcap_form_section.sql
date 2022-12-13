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
        meta__redcap_form_id,
        LEFT(element_preceding_header, 500),
        element_preceding_header
    FROM (
        SELECT DISTINCT
            rf.id meta__redcap_form_id,
            COALESCE(
                (
                    SELECT TOP 1 arms.element_preceding_header
                    FROM [?].dbo.redcap_metadata arms
                    WHERE arms.project_id = arm.project_id
                        AND arms.form_name = arm.form_name
                        AND arms.field_order <= arm.field_order
                        AND arms.element_preceding_header IS NOT NULL
                    ORDER BY arms.field_order DESC
                ),
                ''
            ) AS element_preceding_header
        FROM [?].dbo.redcap_metadata arm
        JOIN warehouse_config.dbo.cfg_redcap_instance ri
            ON ri.datalake_database = '?'
        JOIN warehouse_central.dbo.meta__redcap_project rp
            ON  rp.cfg_redcap_instance_id = ri.id
            AND rp.redcap_project_id = arm.project_id
        JOIN warehouse_central.dbo.meta__redcap_form rf
            ON rf.name = arm.form_name
            AND rf.meta__redcap_project_id = rp.id
    ) x
END"

SET QUOTED_IDENTIFIER ON;
