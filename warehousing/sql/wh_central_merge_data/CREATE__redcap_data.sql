SET QUOTED_IDENTIFIER OFF;
	
CREATE TABLE dbo.redcap_value (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    meta__instance_id INT NOT NULL,
    meta__redcap_project_id INT NOT NULL,
    meta__redcap_arm_id INT NOT NULL,
    meta__redcap_event_id INT NOT NULL,
    meta__redcap_form_id INT NOT NULL,
    meta__redcap_form_section_id INT NOT NULL,
    meta__redcap_field_id INT NOT NULL,
    redcap_participant_id INT NOT NULL,
    [instance] INT,
    text_value NVARCHAR(MAX) NOT NULL,
    datetime_value DATETIME2 NULL,
    date_value DATE NULL,
    time_value TIME NULL,
    int_value INT NULL,
    decimal_value DECIMAL(38,6) NULL,
    meta__redcap_field_enum_id INT NULL,
    enum_name NVARCHAR(500) NULL,
    file_number INT NULL,
    FOREIGN KEY (meta__instance_id) REFERENCES meta__redcap_instance(id),
    FOREIGN KEY (meta__redcap_project_id) REFERENCES meta__redcap_project(id),
    FOREIGN KEY (meta__redcap_arm_id) REFERENCES meta__redcap_arm(id),
    FOREIGN KEY (meta__redcap_event_id) REFERENCES meta__redcap_event(id),
    FOREIGN KEY (meta__redcap_form_id) REFERENCES meta__redcap_form(id),
    FOREIGN KEY (meta__redcap_form_section_id) REFERENCES meta__redcap_form_section(id),
    FOREIGN KEY (meta__redcap_field_id) REFERENCES meta__redcap_field(id),
    FOREIGN KEY (redcap_participant_id) REFERENCES redcap_participant(id),
    FOREIGN KEY (meta__redcap_field_enum_id) REFERENCES meta__redcap_field_enum(id),
);

EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'datalake_redcap_%'
BEGIN 
	INSERT INTO warehouse_central.dbo.redcap_value (
        meta__instance_id,
        meta__redcap_project_id,
        meta__redcap_arm_id,
        meta__redcap_event_id,
        meta__redcap_form_id,
        meta__redcap_form_section_id,
        meta__redcap_field_id,
        redcap_participant_id,
        [instance],
        text_value,
        datetime_value,
        date_value,
        time_value,
        int_value,
        decimal_value,
        meta__redcap_field_enum_id,
        enum_name,
        file_number
    )
    SELECT
        mri.id AS meta__redcap_instance_id,
        mrp.id AS meta__redcap_project_id,
        mra.id AS meta__redcap_arm_id,
        mre.id AS meta__redcap_event_id,
        mrf.id AS meta__redcap_form_id,
        mrfs.id AS meta__redcap_form_section_id,
        mrf2.id AS meta__redcap_field_id,
        rp.id AS redcap_participant_id,
        rd.[instance],
        rd.value,
        CASE
            WHEN TRIM(COALESCE(rd.value, '')) = '' THEN NULL
            WHEN mrf2.type = 'text' AND mrf2.validation_type IN ('date_ymd', 'date_mdy', 'date_dmy') THEN CONVERT(DATETIME2, rd.value, 102)
            WHEN mrf2.type = 'text' AND mrf2.validation_type IN ('datetime_ymd', 'datetime_dmy', 'datetime_mdy', 'datetime_seconds_dmy', 'datetime_seconds_mdy', 'datetime_seconds_ymd') THEN CONVERT(DATETIME2, rd.value, 120)
        END,
        CASE
            WHEN TRIM(COALESCE(rd.value, '')) = '' THEN NULL
            WHEN mrf2.type = 'text' AND mrf2.validation_type IN ('date_ymd', 'date_mdy', 'date_dmy') THEN CONVERT(DATE, rd.value, 102)
            WHEN mrf2.type = 'text' AND mrf2.validation_type IN ('datetime_ymd', 'datetime_dmy', 'datetime_mdy', 'datetime_seconds_dmy', 'datetime_seconds_mdy', 'datetime_seconds_ymd') THEN CONVERT(DATE, rd.value, 120)
        END,
        CASE
            WHEN TRIM(COALESCE(rd.value, '')) = '' THEN NULL
            WHEN mrf2.type = 'text' AND mrf2.validation_type IN ('time', 'time_mm_ss') THEN CONVERT(TIME, rd.value, 108)
            WHEN mrf2.type = 'text' AND mrf2.validation_type IN ('datetime_ymd', 'datetime_dmy', 'datetime_mdy', 'datetime_seconds_dmy', 'datetime_seconds_mdy', 'datetime_seconds_ymd') THEN CONVERT(TIME, rd.value, 120)
        END,
        CASE
            WHEN TRIM(COALESCE(rd.value, '')) = '' THEN NULL
            WHEN (mrf2.type = 'text' AND mrf2.validation_type IN ('int', 'integer')) OR mrf2.type = 'slider' THEN CONVERT(INT, rd.value)
        END,
        CASE
            WHEN TRIM(COALESCE(rd.value, '')) = '' THEN NULL
            WHEN mrf2.type = 'text' AND mrf2.validation_type IN ('float', 'number', 'number_1dp', 'number_2dp', 'number_3dp', 'number_4dp') THEN CONVERT(DEC(38,6), rd.value)
            WHEN mrf2.type = 'text' AND mrf2.validation_type IN ('number_comma_decimal', 'number_1dp_comma_decimal', 'number_2dp_comma_decimal', 'number_3dp_comma_decimal', 'number_4dp_comma_decimal') THEN CONVERT(DEC(38,6), REPLACE(rd.value, ',', ''))
        END,
        CASE
            WHEN mrf2.type IN ('select', 'radio', 'checkbox') THEN mrfe.id
        END,
        CASE
            WHEN mrf2.type IN ('select', 'radio', 'checkbox') THEN mrfe.name
            WHEN mrf2.type IN ('yesno') THEN
                CASE
                    WHEN rd.value = 0 THEN 'No'
                    WHEN rd.value = 1 THEN 'Yes'
                END
            WHEN mrf2.type IN ('truefalse') THEN
                CASE
                    WHEN rd.value = 0 THEN 'False'
                    WHEN rd.value = 1 THEN 'True'
                END
        END,
        CASE
            WHEN mrf2.type = 'file'	THEN rd.value
        END        
    FROM datalake_redcap_uhl.dbo.redcap_data rd 
    JOIN warehouse_central.dbo.meta__redcap_instance mri 
        ON mri.datalake_database = 'datalake_redcap_uhl'
    JOIN warehouse_central.dbo.meta__redcap_project mrp 
        ON mrp.meta__instance_id = mri.id 
        AND mrp.redcap_project_id = rd.project_id 
    JOIN warehouse_central.dbo.meta__redcap_arm mra 
        ON mra.meta__redcap_project_id = mrp.id 
    JOIN warehouse_central.dbo.meta__redcap_event mre 
        ON mre.meta__redcap_arm_id = mra.id 
        AND mre.redcap_event_id = rd.event_id 
    JOIN warehouse_central.dbo.meta__redcap_form mrf 
        ON mrf.meta__project_id = mrp.id 
    JOIN warehouse_central.dbo.meta__redcap_form_section mrfs 
        ON mrfs.meta__form_id = mrf.id 
    JOIN warehouse_central.dbo.meta__redcap_field mrf2 
        ON mrf2.meta__form_section_id = mrfs.id
        AND mrf2.name = rd.field_name
    JOIN warehouse_central.dbo.redcap_participant rp 
        ON rp.meta__redcap_project_id = mrp.id 
        AND rp.record = rd.record
    LEFT JOIN warehouse_central.dbo.meta__redcap_field_enum mrfe 
        ON mrfe.meta__redcap_field_id = mrf2.id
        AND mrfe.value = rd.value
END"

SET QUOTED_IDENTIFIER ON;
