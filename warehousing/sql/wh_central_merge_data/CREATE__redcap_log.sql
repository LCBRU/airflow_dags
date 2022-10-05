SET QUOTED_IDENTIFIER OFF

IF OBJECT_ID(N'redcap_log') IS NOT NULL
    DROP TABLE redcap_log

CREATE TABLE redcap_log (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    meta__redcap_field_id INT NULL,
    meta__redcap_field_enum_id INT NULL,
    meta__redcap_event_id INT NOT NULL,
    redcap_participant_id INT NOT NULL,
    username NVARCHAR(500) NOT NULL,
    field_name NVARCHAR(500) NOT NULL,
    action_datetime DATETIME NOT NULL,
    action_type NVARCHAR(500) NOT NULL,
    data_value NVARCHAR(MAX) NOT NULL,
    [instance] INT NOT NULL,
    INDEX idx__redcap_log__meta__redcap_field_id (meta__redcap_field_id),
    INDEX idx__redcap_log__meta__redcap_field_enum_id (meta__redcap_field_enum_id),
    INDEX idx__redcap_log__meta__redcap_event_id (meta__redcap_event_id),
    INDEX idx__redcap_log__redcap_participant_id (redcap_participant_id),
    FOREIGN KEY (meta__redcap_field_id) REFERENCES meta__redcap_field(id),
    FOREIGN KEY (meta__redcap_field_enum_id) REFERENCES meta__redcap_field_enum(id),
    FOREIGN KEY (meta__redcap_event_id) REFERENCES meta__redcap_event(id),
    FOREIGN KEY (redcap_participant_id) REFERENCES redcap_participant(id),
)

IF OBJECT_ID(N'temp_redcap_log') IS NOT NULL
    DROP TABLE temp_redcap_log

CREATE TABLE temp_redcap_log (
    datalake_database NVARCHAR(500) NOT NULL,
    log_event_id INT NOT NULL,
    project_id INT NOT NULL,
    record NVARCHAR(500) NOT NULL,
    event_id INT NOT NULL,
    username NVARCHAR(500) NOT NULL,
    action_datetime DATETIME NOT NULL,
    action_type NVARCHAR(500) NOT NULL,
    field_name_value NVARCHAR(MAX) NOT NULL,
    [instance] INT NULL,
    field_name NVARCHAR(MAX) NULL,
    value NVARCHAR(MAX) NULL,
    option_value NVARCHAR(500) NULL,
    INDEX idx__temp_redcap_log__log_event_id__datalake_database (log_event_id, datalake_database),
)


EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'datalake_redcap_%'
BEGIN
	INSERT INTO temp_redcap_log (
		datalake_database,
		log_event_id,
        project_id,
        record,
        event_id,
        username,
        action_datetime,
        action_type,
        field_name_value
    )
    SELECT
        '?' datalake_database,
        log_event_id,
        project_id,
        pk AS record,
        event_id,
        [user] username,
		CONVERT(DATETIME, STUFF(STUFF(STUFF(CONVERT(VARCHAR, ts),13,0,':'),11,0,':'),9,0,' ')) [action_datetime],
		[event] action_type,
		value
    FROM [?].dbo.redcap_log_event
    CROSS APPLY STRING_SPLIT(REPLACE(data_values, ',' + CHAR(10), '|') , '|')
    WHERE object_type = 'redcap_data'
        AND TRIM(event) IN ('DELETE', 'INSERT', 'UPDATE')
        AND pk IS NOT NULL
        AND event_id IS NOT NULL
END"



---------------------------
--   Set Instance
---------------------------

UPDATE t
	SET [instance] = x.[instance]
FROM temp_redcap_log t
JOIN (
	SELECT datalake_database, log_event_id, CONVERT(INT, SUBSTRING(field_name_value, 13, PATINDEX('%[0-9]]%', field_name_value) - 12)) [instance]
	FROM temp_redcap_log
	WHERE field_name_value LIKE '![instance = %[1-9]!]%' ESCAPE '!'
) x ON x.datalake_database = t.datalake_database
	AND x.log_event_id = t.log_event_id

	
UPDATE t
	SET [instance] = 1
FROM temp_redcap_log t
WHERE t.[instance] IS NULL

DELETE FROM temp_redcap_log
WHERE field_name_value LIKE '![instance = %[1-9]!]%' ESCAPE '!'

--------------------------------
--   Split field name and value
--------------------------------

UPDATE t
SET field_name = TRIM(REPLACE(LEFT(field_name_value, CHARINDEX('=', field_name_value)), '=', '')),
	value = TRIM(REPLACE(RIGHT(field_name_value, LEN(field_name_value) - CHARINDEX('=', field_name_value)), CHAR(39), ''))
FROM temp_redcap_log t


-----------------------------------------
--   Extract Option valie from field name
-----------------------------------------

UPDATE t
SET option_value = TRIM(RIGHT(LEFT(field_name, LEN(field_name) - 1), LEN(field_name) - CHARINDEX('(', field_name) - 1)),
	field_name = LEFT(field_name, CHARINDEX('(', field_name) - 1)
FROM temp_redcap_log t
WHERE field_name LIKE '%(%)'


-----------------------------------------
--   Insert into Log
-----------------------------------------

INSERT INTO warehouse_central.dbo.redcap_log (meta__redcap_field_id, meta__redcap_field_enum_id, meta__redcap_event_id, redcap_participant_id, username, field_name, action_datetime, action_type, data_value, [instance])
SELECT
    field.meta__redcap_field_id field_id,
    rfe.id,
    mre.id event_id,
    rp.id participant_id,
    x.username,
    x.field_name,
    x.action_datetime,
    x.action_type,
    x.value,
    x.[instance]
FROM warehouse_central.dbo.temp_redcap_log x
JOIN warehouse_central.dbo.meta__redcap_instance mri 
    ON mri.datalake_database = x.datalake_database
JOIN warehouse_central.dbo.meta__redcap_project mrp 
    ON mrp.meta__redcap_instance_id = mri.id 
    AND mrp.redcap_project_id = x.project_id 
JOIN warehouse_central.dbo.meta__redcap_arm mra 
    ON mra.meta__redcap_project_id = mrp.id 
JOIN warehouse_central.dbo.meta__redcap_event mre 
    ON mre.meta__redcap_arm_id = mra.id 
    AND mre.redcap_event_id = x.event_id 
JOIN warehouse_central.dbo.redcap_participant rp 
    ON rp.meta__redcap_project_id = mrp.id 
    AND rp.record = x.record
LEFT JOIN (
	SELECT
		mrf.meta__redcap_project_id,
		mrf2.id AS meta__redcap_field_id,
		mrf2.name
	FROM warehouse_central.dbo.meta__redcap_form mrf
	JOIN warehouse_central.dbo.meta__redcap_form_section mrfs
        ON mrfs.meta__redcap_form_id = mrf.id
	JOIN warehouse_central.dbo.meta__redcap_field mrf2
        ON mrf2.meta__redcap_form_section_id = mrfs.id
) field
	ON field.meta__redcap_project_id = mrp.id
	AND field.name = x.field_name
LEFT JOIN warehouse_central.dbo.meta__redcap_field_enum rfe
	ON rfe.meta__redcap_field_id = field.meta__redcap_field_id
	AND rfe.value = x.option_value
	
IF OBJECT_ID(N'temp_redcap_log') IS NOT NULL
    DROP TABLE temp_redcap_log

SET QUOTED_IDENTIFIER ON;
