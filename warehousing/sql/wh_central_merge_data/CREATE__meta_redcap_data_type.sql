CREATE TABLE dbo.meta__redcap_data_type (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    element_type VARCHAR(50) NOT NULL,
    element_validation_type VARCHAR(50) NOT NULL,
    is_datetime BIT NOT NULL,
    is_date BIT NOT NULL,
    is_time BIT NOT NULL,
    is_int BIT NOT NULL,
    is_decimal BIT NOT NULL,
    is_enum BIT NOT NULL,
    is_boolean BIT NOT NULL,
    is_file BIT NOT NULL,
    datetime_format INT,
    INDEX idx__meta__redcap_data_type__element_type__element_validation_checktype (element_type, element_validation_type),
);

INSERT INTO meta__redcap_data_type (element_type, element_validation_type, is_datetime, is_date, is_time, is_int, is_decimal, is_enum, is_boolean, is_file, datetime_format)
SELECT 'text', '', 0, 0, 0, 0, 0, 0, 0, 0, NULL
UNION
SELECT 'text', 'date_ymd', 1, 1, 0, 0, 0, 0, 0, 0, 102
UNION
SELECT 'text', 'date_mdy', 1, 1, 0, 0, 0, 0, 0, 0, 102
UNION
SELECT 'text', 'date_dmy', 1, 1, 0, 0, 0, 0, 0, 0, 102
UNION
SELECT 'text', 'datetime_ymd', 1, 1, 1, 0, 0, 0, 0, 0, 120
UNION
SELECT 'text', 'datetime_dmy', 1, 1, 1, 0, 0, 0, 0, 0, 120
UNION
SELECT 'text', 'datetime_mdy', 1, 1, 1, 0, 0, 0, 0, 0, 120
UNION
SELECT 'text', 'datetime_seconds_dmy', 1, 1, 1, 0, 0, 0, 0, 0, 120
UNION
SELECT 'text', 'datetime_seconds_mdy', 1, 1, 1, 0, 0, 0, 0, 0, 120
UNION
SELECT 'text', 'datetime_seconds_ymd', 1, 1, 1, 0, 0, 0, 0, 0, 120
UNION
SELECT 'text', 'datetime_seconds_ymd', 1, 1, 1, 0, 0, 0, 0, 0, 120
UNION
SELECT 'text', 'time', 0, 0, 1, 0, 0, 0, 0, 0, 108
UNION
SELECT 'text', 'time_mm_ss', 0, 0, 1, 0, 0, 0, 0, 0, 108
UNION
SELECT 'text', 'int', 0, 0, 0, 1, 1, 0, 0, 0, NULL
UNION
SELECT 'text', 'integer', 0, 0, 0, 1, 1, 0, 0, 0, NULL
UNION
SELECT 'slider', '', 0, 0, 0, 1, 1, 0, 0, 0, NULL
UNION
SELECT 'text', 'float', 0, 0, 0, 0, 1, 0, 0, 0, NULL
UNION
SELECT 'text', 'number', 0, 0, 0, 0, 1, 0, 0, 0, NULL
UNION
SELECT 'text', 'number_1dp', 0, 0, 0, 0, 1, 0, 0, 0, NULL
UNION
SELECT 'text', 'number_2dp', 0, 0, 0, 0, 1, 0, 0, 0, NULL
UNION
SELECT 'text', 'number_3dp', 0, 0, 0, 0, 1, 0, 0, 0, NULL
UNION
SELECT 'text', 'number_4dp', 0, 0, 0, 0, 1, 0, 0, 0, NULL
UNION
SELECT 'text', 'number_comma_decimal', 0, 0, 0, 0, 1, 0, 0, 0, NULL
UNION
SELECT 'text', 'number_1dp_comma_decimal', 0, 0, 0, 0, 1, 0, 0, 0, NULL
UNION
SELECT 'text', 'number_2dp_comma_decimal', 0, 0, 0, 0, 1, 0, 0, 0, NULL
UNION
SELECT 'text', 'number_3dp_comma_decimal', 0, 0, 0, 0, 1, 0, 0, 0, NULL
UNION
SELECT 'text', 'number_4dp_comma_decimal', 0, 0, 0, 0, 1, 0, 0, 0, NULL
UNION
SELECT 'select', '', 0, 0, 0, 0, 0, 1, 0, 0, NULL
UNION
SELECT 'radio', '', 0, 0, 0, 0, 0, 1, 0, 0, NULL
UNION
SELECT 'checkbox', '', 0, 0, 0, 0, 0, 1, 0, 0, NULL
UNION
SELECT 'yesno', '', 0, 0, 0, 0, 0, 1, 1, 0, NULL
UNION
SELECT 'truefalse', '', 0, 0, 0, 0, 0, 1, 1, 0, NULL
UNION
SELECT 'file', '', 0, 0, 0, 0, 0, 0, 0, 1, NULL
