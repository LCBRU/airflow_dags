SET QUOTED_IDENTIFIER OFF;

CREATE TABLE dbo.redcap_value (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    meta__redcap_instance_id INT NOT NULL,
    meta__redcap_project_id INT NOT NULL,
    meta__redcap_arm_id INT NOT NULL,
    meta__redcap_event_id INT NOT NULL,
    meta__redcap_form_id INT NOT NULL,
    meta__redcap_form_section_id INT NOT NULL,
    meta__redcap_field_id INT NOT NULL,
    redcap_participant_id INT NOT NULL,
    meta__redcap_field_enum_id INT NULL,
    [instance] INT NULL,
    text_value NVARCHAR(MAX) NOT NULL,
    datetime_value DATETIME2 NULL,
    date_value DATE NULL,
    time_value TIME NULL,
    int_value INT NULL,
    decimal_value DECIMAL(38,6) NULL,
    boolean_value BIT NULL,
    file_number INT NULL,
    FOREIGN KEY (meta__redcap_instance_id) REFERENCES meta__redcap_instance(id),
    FOREIGN KEY (meta__redcap_project_id) REFERENCES meta__redcap_project(id),
    FOREIGN KEY (meta__redcap_arm_id) REFERENCES meta__redcap_arm(id),
    FOREIGN KEY (meta__redcap_event_id) REFERENCES meta__redcap_event(id),
    FOREIGN KEY (meta__redcap_form_id) REFERENCES meta__redcap_form(id),
    FOREIGN KEY (meta__redcap_form_section_id) REFERENCES meta__redcap_form_section(id),
    FOREIGN KEY (meta__redcap_field_id) REFERENCES meta__redcap_field(id),
    FOREIGN KEY (redcap_participant_id) REFERENCES redcap_participant(id),
    FOREIGN KEY (meta__redcap_field_enum_id) REFERENCES meta__redcap_field_enum(id),
    INDEX idx__redcap_value__meta__redcap_instance_id (meta__redcap_instance_id),
    INDEX idx__redcap_value__meta__redcap_project_id (meta__redcap_project_id),
    INDEX idx__redcap_value__meta__redcap_arm_id (meta__redcap_arm_id),
    INDEX idx__redcap_value__meta__redcap_event_id (meta__redcap_event_id),
    INDEX idx__redcap_value__meta__redcap_form_id (meta__redcap_form_id),
    INDEX idx__redcap_value__meta__redcap_form_section_id (meta__redcap_form_section_id),
    INDEX idx__redcap_value__meta__redcap_field_id (meta__redcap_field_id),
    INDEX idx__redcap_value__redcap_participant_id (redcap_participant_id),
    INDEX idx__redcap_value__meta__redcap_field_enum_id (meta__redcap_field_enum_id),
);

EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'datalake_redcap_%'
BEGIN 
	INSERT INTO warehouse_central.dbo.redcap_value (
        meta__redcap_instance_id,
        meta__redcap_project_id,
        meta__redcap_arm_id,
        meta__redcap_event_id,
        meta__redcap_form_id,
        meta__redcap_form_section_id,
        meta__redcap_field_id,
        redcap_participant_id,
        [instance],
        text_value
    )
    SELECT
        mri.id,
        mrp.id,
        mra.id,
        mre.id,
        mrf.id,
        mrfs.id,
        mrf2.id,
        rp.id,
        rd.[instance],
        rd.value
    FROM datalake_redcap_uhl.dbo.redcap_data rd 
    JOIN warehouse_central.dbo.meta__redcap_instance mri 
        ON mri.datalake_database = 'datalake_redcap_uhl'
    JOIN warehouse_central.dbo.meta__redcap_project mrp 
        ON mrp.meta__redcap_instance_id = mri.id 
        AND mrp.redcap_project_id = rd.project_id 
    JOIN warehouse_central.dbo.meta__redcap_arm mra 
        ON mra.meta__redcap_project_id = mrp.id 
    JOIN warehouse_central.dbo.meta__redcap_event mre 
        ON mre.meta__redcap_arm_id = mra.id 
        AND mre.redcap_event_id = rd.event_id 
    JOIN warehouse_central.dbo.meta__redcap_form mrf 
        ON mrf.meta__redcap_project_id = mrp.id 
    JOIN warehouse_central.dbo.meta__redcap_form_section mrfs 
        ON mrfs.meta__redcap_form_id = mrf.id 
    JOIN warehouse_central.dbo.meta__redcap_field mrf2 
        ON mrf2.meta__redcap_form_section_id = mrfs.id
        AND mrf2.name = rd.field_name
    JOIN warehouse_central.dbo.redcap_participant rp 
        ON rp.meta__redcap_project_id = mrp.id 
        AND rp.record = rd.record
END"

UPDATE warehouse_central.dbo.redcap_value
SET datetime_value = CASE
        WHEN mrf2.type = 'text'
            AND mrf2.validation_type IN ('date_ymd', 'date_mdy', 'date_dmy')
            THEN TRY_CONVERT(DATETIME2, rv.text_value, 102)
        WHEN mrf2.type = 'text'
            AND mrf2.validation_type IN ('datetime_ymd', 'datetime_dmy', 'datetime_mdy', 'datetime_seconds_dmy', 'datetime_seconds_mdy', 'datetime_seconds_ymd')
            THEN TRY_CONVERT(DATETIME2, rv.text_value, 120)
    END,
    date_value = CASE
        WHEN mrf2.type = 'text'
            AND mrf2.validation_type IN ('date_ymd', 'date_mdy', 'date_dmy')
            THEN TRY_CONVERT(DATE, rv.text_value, 102)
        WHEN mrf2.type = 'text'
            AND mrf2.validation_type IN ('datetime_ymd', 'datetime_dmy', 'datetime_mdy', 'datetime_seconds_dmy', 'datetime_seconds_mdy', 'datetime_seconds_ymd')
            THEN TRY_CONVERT(DATE, rv.text_value, 120)
    END,
    time_value = CASE
        WHEN mrf2.type = 'text'
            AND mrf2.validation_type IN ('time', 'time_mm_ss')
            THEN TRY_CONVERT(TIME, rv.text_value, 108)
        WHEN mrf2.type = 'text'
            AND mrf2.validation_type IN ('datetime_ymd', 'datetime_dmy', 'datetime_mdy', 'datetime_seconds_dmy', 'datetime_seconds_mdy', 'datetime_seconds_ymd')
            THEN TRY_CONVERT(TIME, rv.text_value, 120)
    END,
    int_value = CASE
        WHEN (mrf2.type = 'text' AND mrf2.validation_type IN ('int', 'integer'))
            OR mrf2.type = 'slider'
            THEN TRY_CONVERT(INT, rv.text_value)
    END,
    decimal_value = CASE
        WHEN mrf2.type = 'text'
            AND mrf2.validation_type IN ('float', 'number', 'number_1dp', 'number_2dp', 'number_3dp', 'number_4dp', 'number_comma_decimal', 'number_1dp_comma_decimal', 'number_2dp_comma_decimal', 'number_3dp_comma_decimal', 'number_4dp_comma_decimal')
            THEN TRY_CONVERT(DEC(38,6), REPLACE(rv.text_value, ',', ''))
    END,
    meta__redcap_field_enum_id = CASE
        WHEN mrf2.type IN ('select', 'radio', 'checkbox', 'yesno', 'truefalse') THEN mrfe.id
    END,
    boolean_value = CASE
        WHEN mrf2.type IN ('yesno', 'truefalse') THEN
            CASE
                WHEN rv.text_value = '0' THEN 0
                WHEN rv.text_value = '1' THEN 1
            END
    END,
    file_number = CASE WHEN mrf2.type = 'file' THEN rv.text_value END        
FROM warehouse_central.dbo.redcap_value rv
JOIN warehouse_central.dbo.meta__redcap_field mrf2 
    ON mrf2.id = rv.meta__redcap_field_id 
LEFT JOIN warehouse_central.dbo.meta__redcap_field_enum mrfe 
    ON mrfe.meta__redcap_field_id = mrf2.id
    AND mrfe.value = rv.text_value


SET QUOTED_IDENTIFIER ON;
