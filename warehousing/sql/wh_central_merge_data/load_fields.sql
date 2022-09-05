-- Active: 1662037517883@@uhlsqlbriccs01@1433@warehouse_central

DELETE FROM wh_study_scad.dbo.redcap_field_enum;
DELETE FROM wh_study_scad.dbo.redcap_field;
DELETE FROM wh_study_scad.dbo.redcap_form_section;
DELETE FROM wh_study_scad.dbo.redcap_form;

CREATE TABLE #fields (
    field_name VARCHAR(100),
    form_name VARCHAR(100),
    field_order INT,
    field_units VARCHAR(50),
    element_preceding_header VARCHAR(MAX),
    element_type VARCHAR(50),
    element_label VARCHAR(MAX),
    element_enum VARCHAR(MAX),
    element_validation_type VARCHAR(255),
    form_id INT,
    form_section_id INT,
    field_id INT
)

INSERT INTO #fields (
    field_name,
    form_name,
    field_order,
    field_units,
    element_preceding_header,
    element_type,
    element_label,
    element_enum,
    element_validation_type
)
SELECT
    rm.field_name,
    rm.form_name,
    rm.field_order,
    rm.field_units,
    COALESCE(
        (
            SELECT TOP 1 rms.element_preceding_header
            FROM datalake_redcap_uhl.dbo.redcap_metadata rms
            WHERE rms.project_id = rm.project_id
                AND rms.form_name = rm.form_name
                AND rms.field_order <= rm.field_order
                AND rms.element_preceding_header IS NOT NULL
            ORDER BY rms.field_order DESC
        ),
        ''
    ) AS element_preceding_header,
    rm.element_type,
    rm.element_label,
    rm.element_enum,
    rm.element_validation_type
FROM datalake_redcap_uhl.dbo.redcap_metadata rm
WHERE rm.project_id = 31

INSERT INTO wh_study_scad.dbo.redcap_form (name)
SELECT DISTINCT form_name
FROM #fields

UPDATE f
SET form_id = rf.id
FROM #fields f
JOIN wh_study_scad.dbo.redcap_form rf
    ON rf.name = f.form_name

INSERT INTO wh_study_scad.dbo.redcap_form_section (form_id, name)
SELECT DISTINCT form_id, element_preceding_header
FROM #fields

UPDATE f
SET form_section_id = rfs.id
FROM #fields f
JOIN wh_study_scad.dbo.redcap_form_section rfs
    ON rfs.name = f.element_preceding_header

INSERT INTO wh_study_scad.dbo.redcap_field (form_section_id, ordinal, name, label, type, units, validation_type)
SELECT
    form_section_id,
    field_order,
    field_name,
    element_label,
    element_type,
    units,
    element_validation_type
FROM #fields

UPDATE f
SET field_id = rf.id
FROM #fields f
JOIN wh_study_scad.dbo.redcap_field rf
    ON rf.form_section_id = f.form_section_id
    AND rf.ordinal = f.field_order
    AND rf.name = f.field_name


INSERT INTO wh_study_scad.dbo.redcap_field_enum (field_id, value, name)
SELECT
    f.field_id,
    CONVERT(INT, TRIM(LEFT(value, PATINDEX('%,%', value + ',') - 1))),
    TRIM(RIGHT(value, LEN(value) - PATINDEX('%,%', value + ',')))
FROM #fields f
CROSS APPLY STRING_SPLIT(REPLACE(f.element_enum, '\n', '|'), '|')
WHERE LEN(TRIM(COALESCE(f.element_enum, ''))) > 0
    AND element_type <> 'calc'


SELECT COUNT(*)
FROM (
	SELECT
		rl.log_event_id,
		CONVERT(DATETIME2, FORMAT(rl.ts, '####-##-## ##:##:##'), 120) AS log_datetime,
		rl.project_id,
		rl.[user],
		rl.event,
		rl.pk AS record,
		TRIM(LEFT(value, PATINDEX('%=%', value + '=') - 1)) field_name,
		TRIM(REPLACE(RIGHT(value, LEN(value) - PATINDEX('%=%', value + '=')), '''', '')) value
	FROM datalake_redcap_uhl.dbo.redcap_log_event rl
		CROSS APPLY STRING_SPLIT(REPLACE(rl.data_values, CHAR(10), ''), ',')
	WHERE rl.event IN ('INSERT', 'UPDATE', 'DELETE')
		AND rl.object_type = 'redcap_data'
		AND LEN(TRIM(COALESCE(rl.data_values, ''))) > 0
) x
;
