SET QUOTED_IDENTIFIER OFF;

CREATE TABLE meta__redcap_field (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    meta__redcap_form_section_id INT NOT NULL,
    meta__redcap_data_type_id INT NOT NULL,
    ordinal INT NOT NULL,
    name NVARCHAR(500) NOT NULL,
    label NVARCHAR(MAX) NULL,
    units VARCHAR(50) NULL,
    INDEX idx__redcap_field__redcap_form_section (meta__redcap_form_section_id),
    INDEX idx__redcap_field__meta__redcap_data_type_id (meta__redcap_data_type_id),
    INDEX idx__redcap_field__ordinal (ordinal),
    INDEX idx__redcap_field__name (name),
    FOREIGN KEY (meta__redcap_form_section_id) REFERENCES meta__redcap_form_section(id),
    FOREIGN KEY (meta__redcap_data_type_id) REFERENCES meta__redcap_data_type(id),
    UNIQUE (meta__redcap_form_section_id, name)
);

EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'datalake_redcap_%'
BEGIN 
	INSERT INTO warehouse_central.dbo.meta__redcap_field (meta__redcap_form_section_id, meta__redcap_data_type_id, ordinal, name, label, units)
    SELECT
        rfs.id,
        rdt.id,
        rfd.field_order,
        rfd.field_name,
        rfd.element_label,
        rfd.field_units
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
    JOIN warehouse_central.dbo.meta__redcap_data_type rdt
        ON rdt.element_type = rfd.element_type
        AND rdt.element_validation_type = ISNULL(rfd.element_validation_type, '')
    JOIN warehouse_central.dbo.meta__redcap_project rp
        ON  rp.meta__redcap_instance_id = ri.id
        AND rp.redcap_project_id = rfd.project_id
    JOIN warehouse_central.dbo.meta__redcap_form rf
        ON rf.name = rfd.form_name
        AND rf.meta__redcap_project_id = rp.id
    JOIN warehouse_central.dbo.meta__redcap_form_section rfs
        ON rfs.name_index = LEFT(rfd.element_preceding_header, 500)
        AND rfs.name = rfd.element_preceding_header
        AND rfs.meta__redcap_form_id = rf.id
END"

SET QUOTED_IDENTIFIER ON;
