SET QUOTED_IDENTIFIER OFF

CREATE TABLE meta__redcap_field_enum (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    meta__redcap_field_id INT NOT NULL,
    value NVARCHAR(500) NOT NULL,
    name NVARCHAR(500) NOT NULL,
    INDEX idx__redcap_field_enum__field (meta__redcap_field_id),
    INDEX idx__redcap_field_enum__name (name),
    UNIQUE (meta__redcap_field_id, value)
)

EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'datalake_redcap_%'
BEGIN
	INSERT INTO warehouse_central.dbo.meta__redcap_field_enum (meta__redcap_field_id, value, name)
    SELECT
        rfd.id,
        rfe.value,
        rfe.name
    FROM (
        SELECT
            '?' datalake_database,
            e.project_id,
            e.field_name,
            e.field_order,
            TRIM(CASE
                WHEN comma_pos > 0 THEN LEFT(value, comma_pos - 1)
                ELSE value
            END) value,
            TRIM(CASE
                WHEN comma_pos > 0 THEN RIGHT(value, LEN(value) - comma_pos)
                ELSE value
            END) name
        FROM (
            SELECT
                f.project_id,
                f.field_name,
                f.element_enum,
                f.field_order,
                value,
                PATINDEX('%,%', value) comma_pos
            FROM [?].dbo.redcap_metadata f
            CROSS APPLY STRING_SPLIT(REPLACE(f.element_enum, '\n', '|'), '|')
            WHERE LEN(TRIM(COALESCE(f.element_enum, ''))) > 0
                AND element_type NOT IN ('calc', 'slider')
                AND LEN(TRIM(COALESCE(value, ''))) > 0
        ) e
    ) rfe
    JOIN warehouse_central.dbo.meta__redcap_instance ri
        ON ri.datalake_database = rfe.datalake_database
    JOIN warehouse_central.dbo.meta__redcap_project rp
        ON  rp.meta__instance_id = ri.id
        AND rp.redcap_project_id = rfe.project_id
    JOIN warehouse_central.dbo.meta__redcap_form rf
        ON rf.meta__project_id = rp.id
    JOIN warehouse_central.dbo.meta__redcap_form_section rfs
        ON rfs.meta__form_id = rf.id
    JOIN warehouse_central.dbo.meta__redcap_field rfd
        ON rfd.meta__form_section_id = rfs.id
        AND rfd.name = rfe.field_name
        AND rfd.ordinal = rfe.field_order
END"

INSERT INTO warehouse_central.dbo.meta__redcap_field_enum (meta__redcap_field_id, value, name)
SELECT
	mrf.id,
	0,
	'No'
FROM meta__redcap_field mrf 
WHERE type = 'yesno'

UNION

SELECT
	mrf.id,
	1,
	'Yes'
FROM meta__redcap_field mrf 
WHERE type = 'yesno'

UNION

SELECT
	mrf.id,
	0,
	'False'
FROM meta__redcap_field mrf 
WHERE type = 'truefalse'

UNION

SELECT
	mrf.id,
	1,
	'True'
FROM meta__redcap_field mrf 
WHERE type = 'truefalse'
;

SET QUOTED_IDENTIFIER ON;
