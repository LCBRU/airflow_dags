SET QUOTED_IDENTIFIER OFF;

CREATE TABLE meta__redcap_field (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    meta__form_section_id INT NOT NULL,
    ordinal INT NOT NULL,
    name NVARCHAR(500) NOT NULL,
    label NVARCHAR(MAX) NULL,
    type VARCHAR(50) NOT NULL,
    units VARCHAR(50) NULL,
    validation_type VARCHAR(255) NULL,
    INDEX idx__redcap_field__form_section (meta__form_section_id),
    INDEX idx__redcap_field__ordinal (ordinal),
    INDEX idx__redcap_field__name (name),
    FOREIGN KEY (meta__form_section_id) REFERENCES meta__redcap_form_section(id),
    UNIQUE (meta__form_section_id, name)
);

EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'datalake_redcap_%'
BEGIN 
	INSERT INTO warehouse_central.dbo.meta__redcap_field (meta__form_section_id, ordinal, name, label, type, units, validation_type)
    SELECT
        rfs.id,
        field_order,
        field_name,
        element_label,
        element_type,
        field_units,
        element_validation_type
    FROM (
        SELECT DISTINCT
            '?' datalake_database,
            project_id,
            form_name,
            field_order,
            field_name,
            element_label,
            element_type,
            field_units,
            element_validation_type,
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
    ) rfd
    JOIN warehouse_central.dbo.meta__redcap_instance ri
        ON ri.datalake_database = rfd.datalake_database
    JOIN warehouse_central.dbo.meta__redcap_project rp
        ON  rp.meta__instance_id = ri.id
        AND rp.redcap_project_id = rfd.project_id
    JOIN warehouse_central.dbo.meta__redcap_form rf
        ON rf.name = rfd.form_name
        AND rf.meta__project_id = rp.id
    JOIN warehouse_central.dbo.meta__redcap_form_section rfs
        ON rfs.name_index = LEFT(rfd.element_preceding_header, 500)
        AND rfs.name = rfd.element_preceding_header
        AND rfs.meta__form_id = rf.id
END"

SET QUOTED_IDENTIFIER ON;
