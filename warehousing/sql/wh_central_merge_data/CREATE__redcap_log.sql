SET QUOTED_IDENTIFIER OFF

CREATE TABLE redcap_log (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    meta__redcap_field_id INT NOT NULL,
    meta__redcap_event_id INT NOT NULL,
    redcap_participant_id INT NOT NULL,
    username NVARCHAR(500) NOT NULL,
    action_datetime DATETIME NOT NULL,
    action_type NVARCHAR(500) NOT NULL,
    data_value NVARCHAR(500) NOT NULL,
    [instance] INT NOT NULL,
    INDEX idx__redcap_log__meta__redcap_field_id (meta__redcap_field_id),
    INDEX idx__redcap_log__meta__redcap_event_id (meta__redcap_event_id),
    INDEX idx__redcap_log__redcap_participant_id (redcap_participant_id),
    FOREIGN KEY (meta__redcap_field_id) REFERENCES meta__redcap_field(id),
    FOREIGN KEY (meta__redcap_event_id) REFERENCES meta__redcap_event(id),
    FOREIGN KEY (redcap_participant_id) REFERENCES redcap_participant(id),
)

IF OBJECT_ID(N'temp_redcap_log') IS NOT NULL
    DROP TABLE temp_redcap_log

CREATE TABLE temp_redcap_log (
    datalake_database NVARCHAR(500) NOT NULL,
    project_id INT NOT NULL,
    record NVARCHAR(500) NOT NULL,
    event_id INT NOT NULL,
    username NVARCHAR(500) NOT NULL,
    action_datetime DATETIME NOT NULL,
    action_type NVARCHAR(500) NOT NULL,
    field_name NVARCHAR(MAX) NOT NULL,
    data_value NVARCHAR(MAX) NOT NULL,
    [instance] INT NOT NULL
)

EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'datalake_redcap_%'
BEGIN
    INSERT INTO warehouse_central.dbo.temp_redcap_log (datalake_database, project_id, record, event_id, username, action_datetime, action_type, field_name, data_value, [instance])
    SELECT
        '?',
        project_id,
		pk,
		event_id,
        [user],
        CONVERT(DATETIME, STUFF(STUFF(STUFF(CONVERT(VARCHAR, ts),13,0,':'),11,0,':'),9,0,' ')),
        event,
        TRIM(REPLACE(LEFT(data_value, CHARINDEX('=', data_value)), '=', '')),
        TRIM(REPLACE(RIGHT(data_value, LEN(data_value) - CHARINDEX('=', data_value)), CHAR(39), '')),
        [instance]
    FROM (
        SELECT
            project_id,
            pk,
            ts,
            [user],
            event,
            event_id,
            [instance],
            REPLACE(REPLACE(value, CHAR(13), ''), CHAR(10), '') data_value
        FROM (
            SELECT *,
                CASE
                    WHEN [instance] = 1 THEN data_values
                    ELSE SUBSTRING(data_values, 16 + LEN(CONVERT(VARCHAR, [instance])), LEN(data_values))
                END sdv
            FROM (
                SELECT
                    *,
                    CASE WHEN data_values LIKE '![instance = [1-9]!]%' ESCAPE '!' OR data_values LIKE '![instance = [1-9][0-9]!]%' ESCAPE '!' OR data_values LIKE '![instance = [1-9][1-9][0-9]!]%' ESCAPE '!' THEN
                        CONVERT(INT, SUBSTRING(data_values, 13, PATINDEX('%[0-9]]%', data_values) - 12))
                    ELSE 1 END AS [instance]
                FROM [?].dbo.redcap_log_event
                WHERE object_type = 'redcap_data'
                    AND TRIM(event) IN ('DELETE', 'INSERT', 'UPDATE')
                    AND pk IS NOT NULL
                    AND event_id IS NOT NULL
            ) x
        ) y
        CROSS APPLY STRING_SPLIT(sdv, ',')
    ) x
END"

INSERT INTO warehouse_central.dbo.redcap_log (meta__redcap_field_id, meta__redcap_event_id, redcap_participant_id, username, action_datetime, action_type, data_value, [instance])
SELECT
    mrf2.id field_id,
    mre.id event_id,
    rp.id participant_id,
    x.username,
    x.action_datetime,
    x.action_type,
    x.data_value,
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
JOIN warehouse_central.dbo.meta__redcap_form mrf 
    ON mrf.meta__redcap_project_id = mrp.id 
JOIN warehouse_central.dbo.meta__redcap_form_section mrfs 
    ON mrfs.meta__redcap_form_id = mrf.id 
JOIN warehouse_central.dbo.meta__redcap_field mrf2 
    ON mrf2.meta__redcap_form_section_id = mrfs.id
    AND mrf2.name = x.field_name
JOIN warehouse_central.dbo.redcap_participant rp 
    ON rp.meta__redcap_project_id = mrp.id 
    AND rp.record = x.record

IF OBJECT_ID(N'temp_redcap_log') IS NOT NULL
    DROP TABLE temp_redcap_log

SET QUOTED_IDENTIFIER ON;
