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
    meta__redcap_data_type_id INT NULL,
    redcap_file_id INT NULL,
    [instance] INT NOT NULL,
    text_value NVARCHAR(MAX) NOT NULL,
    datetime_value DATETIME2 NULL,
    date_value DATE NULL,
    time_value TIME NULL,
    int_value INT NULL,
    decimal_value DECIMAL(38,6) NULL,
    boolean_value BIT NULL,
    FOREIGN KEY (meta__redcap_instance_id) REFERENCES meta__redcap_instance(id),
    FOREIGN KEY (meta__redcap_project_id) REFERENCES meta__redcap_project(id),
    FOREIGN KEY (meta__redcap_arm_id) REFERENCES meta__redcap_arm(id),
    FOREIGN KEY (meta__redcap_event_id) REFERENCES meta__redcap_event(id),
    FOREIGN KEY (meta__redcap_form_id) REFERENCES meta__redcap_form(id),
    FOREIGN KEY (meta__redcap_form_section_id) REFERENCES meta__redcap_form_section(id),
    FOREIGN KEY (meta__redcap_field_id) REFERENCES meta__redcap_field(id),
    FOREIGN KEY (redcap_participant_id) REFERENCES redcap_participant(id),
    FOREIGN KEY (meta__redcap_field_enum_id) REFERENCES meta__redcap_field_enum(id),
    FOREIGN KEY (meta__redcap_data_type_id) REFERENCES meta__redcap_data_type(id),
    FOREIGN KEY (redcap_file_id) REFERENCES redcap_file(id),
    INDEX idx__redcap_value__meta__redcap_instance_id (meta__redcap_instance_id),
    INDEX idx__redcap_value__meta__redcap_project_id (meta__redcap_project_id),
    INDEX idx__redcap_value__meta__redcap_arm_id (meta__redcap_arm_id),
    INDEX idx__redcap_value__meta__redcap_event_id (meta__redcap_event_id),
    INDEX idx__redcap_value__meta__redcap_form_id (meta__redcap_form_id),
    INDEX idx__redcap_value__meta__redcap_form_section_id (meta__redcap_form_section_id),
    INDEX idx__redcap_value__meta__redcap_field_id (meta__redcap_field_id),
    INDEX idx__redcap_value__redcap_participant_id (redcap_participant_id),
    INDEX idx__redcap_value__meta__redcap_field_enum_id (meta__redcap_field_enum_id),
    INDEX idx__redcap_value__meta__redcap_data_type_id (meta__redcap_data_type_id),
    INDEX idx__redcap_value__redcap_file_id (redcap_file_id),
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
        meta__redcap_data_type_id,
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
        mrdt.id,
        rp.id,
        ISNULL(rd.[instance], 1),
        rd.value
    FROM (
        SELECT DISTINCT *
        FROM [?].dbo.redcap_data
    ) rd 
    JOIN warehouse_central.dbo.meta__redcap_instance mri 
        ON mri.datalake_database = '?'
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
    JOIN warehouse_central.dbo.meta__redcap_data_type mrdt
        ON mrdt.id = mrf2.meta__redcap_data_type_id
    JOIN warehouse_central.dbo.redcap_participant rp 
        ON rp.meta__redcap_project_id = mrp.id 
        AND rp.record = rd.record
END"

UPDATE warehouse_central.dbo.redcap_value
SET datetime_value = CASE WHEN rdt.is_datetime = 1 THEN TRY_CONVERT(DATETIME2, rv.text_value, rdt.datetime_format) END,
    date_value = CASE WHEN rdt.is_date = 1 THEN TRY_CONVERT(DATE, rv.text_value, rdt.datetime_format) END,
    time_value = CASE WHEN rdt.is_time = 1 THEN TRY_CONVERT(TIME, rv.text_value, rdt.datetime_format) END,
    int_value = CASE WHEN rdt.is_int = 1 THEN TRY_CONVERT(INT, rv.text_value) END,
    decimal_value = CASE WHEN rdt.is_decimal = 1 THEN TRY_CONVERT(DEC(38,6), REPLACE(rv.text_value, ',', '')) END,
    meta__redcap_field_enum_id = CASE WHEN rdt.is_enum = 1 THEN mrfe.id END,
    boolean_value = CASE WHEN rdt.is_boolean = 1 THEN TRY_CONVERT(BIT, rv.text_value) END,
    redcap_file_id = CASE WHEN rdt.is_file = 1 THEN rf.id END
FROM warehouse_central.dbo.redcap_value rv
JOIN warehouse_central.dbo.meta__redcap_data_type rdt
	ON rdt.id = rv.meta__redcap_data_type_id
	AND (	rdt.is_datetime = 1
		OR 	rdt.is_date = 1
		OR	rdt.is_time = 1
		OR	rdt.is_int = 1
		OR	rdt.is_decimal = 1
		OR	rdt.is_enum = 1
		OR	rdt.is_boolean = 1
		OR	rdt.is_file = 1
	)
LEFT JOIN warehouse_central.dbo.meta__redcap_field_enum mrfe 
    ON mrfe.meta__redcap_field_id = rv.meta__redcap_field_id 
    AND mrfe.value = rv.text_value
LEFT JOIN warehouse_central.dbo.redcap_file rf 
    ON rf.meta__redcap_instance_id = rv.meta__redcap_instance_id 
    AND rf.doc_id = TRY_CONVERT(INT, rv.text_value)
    AND rdt.is_file = 1


SET QUOTED_IDENTIFIER ON;
