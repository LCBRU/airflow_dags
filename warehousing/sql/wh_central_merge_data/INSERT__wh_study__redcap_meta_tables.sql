SET QUOTED_IDENTIFIER OFF;
	
DECLARE @study_name NVARCHAR(500)
DECLARE @destination_name NVARCHAR(500)
DECLARE @datalake_database NVARCHAR(500)
DECLARE @sql NVARCHAR(MAX) 

DECLARE db_cursor CURSOR FOR 
SELECT DISTINCT study_name, datalake_database
FROM datalake_redcap_project_mappings

OPEN db_cursor  
FETCH NEXT FROM db_cursor INTO @study_name, @datalake_database

WHILE @@FETCH_STATUS = 0  
BEGIN
	SET @destination_name = '[wh_study_' + @study_name + ']'

	CREATE TABLE #fields (
	    redcap_project_id INT,
	    project_name VARCHAR(100),
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
	    field_id INT,
	    project_id INT,
		instance_id INT
	)
	
	SET @sql = "
		INSERT INTO #fields (
			redcap_project_id,
			project_name,
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
			rm.project_id,
			rp.project_name,
		    rm.field_name,
		    rm.form_name,
		    rm.field_order,
		    rm.field_units,
		    COALESCE(
		        (
		            SELECT TOP 1 rms.element_preceding_header
		            FROM dl_db.dbo.redcap_metadata rms
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
		FROM dl_db.dbo.redcap_metadata rm
		JOIN dl_db.dbo.redcap_projects rp
			ON rp.project_id = rm.project_id
		WHERE rm.project_id = 31
		"
	
	SET @sql = REPLACE(REPLACE(@sql, 'study_db', @destination_name), 'dl_db', @datalake_database)
	EXECUTE sp_executesql @sql

	SET @sql = "
		UPDATE f
		SET instance_id = ri.id
		FROM #fields f
		CROSS JOIN study_db.dbo.meta__redcap_instance ri
		WHERE ri.datalake_database = 'dl_db'
		"
	
	SET @sql = REPLACE(REPLACE(@sql, 'study_db', @destination_name), 'dl_db', @datalake_database)
	EXECUTE sp_executesql @sql

	SET @sql = "
		INSERT INTO study_db.dbo.meta__redcap_project (meta_instance_id, redcap_project_id, name)
		SELECT DISTINCT f.instance_id, f.redcap_project_id, f.project_name
		FROM #fields f
		"
	
	SET @sql = REPLACE(REPLACE(@sql, 'study_db', @destination_name), 'dl_db', @datalake_database)
	EXECUTE sp_executesql @sql

	SET @sql = "
		UPDATE f
		SET project_id = rp.id
		FROM #fields f
		JOIN study_db.dbo.meta__redcap_project rp
			ON rp.meta_instance_id = f.instance_id
			AND rp.redcap_project_id = f.redcap_project_id
		"
	
	SET @sql = REPLACE(REPLACE(@sql, 'study_db', @destination_name), 'dl_db', @datalake_database)
	EXECUTE sp_executesql @sql

	SET @sql = "
		INSERT INTO study_db.dbo.meta__redcap_form (meta_project_id, name)
		SELECT DISTINCT project_id, form_name
		FROM #fields
		"
	
	SET @sql = REPLACE(REPLACE(@sql, 'study_db', @destination_name), 'dl_db', @datalake_database)
	EXECUTE sp_executesql @sql

	SET @sql = "
		UPDATE f
		SET form_id = rf.id
		FROM #fields f
		JOIN study_db.dbo.meta__redcap_form rf
		    ON rf.name = f.form_name
		    AND rf.meta_project_id = f.project_id
		"
	
	SET @sql = REPLACE(REPLACE(@sql, 'study_db', @destination_name), 'dl_db', @datalake_database)
	EXECUTE sp_executesql @sql

	SET @sql = "
		INSERT INTO study_db.dbo.meta__redcap_form_section (meta_form_id, name)
		SELECT DISTINCT form_id, element_preceding_header
		FROM #fields
		"
	
	SET @sql = REPLACE(REPLACE(@sql, 'study_db', @destination_name), 'dl_db', @datalake_database)
	EXECUTE sp_executesql @sql

	SET @sql = "
		UPDATE f
		SET form_section_id = rfs.id
		FROM #fields f
		JOIN study_db.dbo.meta__redcap_form_section rfs
		    ON rfs.name = f.element_preceding_header
		    AND rfs.meta_form_id = f.form_id
		"
	
	SET @sql = REPLACE(REPLACE(@sql, 'study_db', @destination_name), 'dl_db', @datalake_database)
	EXECUTE sp_executesql @sql

	SET @sql = "
		INSERT INTO study_db.dbo.meta__redcap_field (meta_form_section_id, ordinal, name, label, type, units, validation_type)
		SELECT
		    form_section_id,
		    field_order,
		    field_name,
		    element_label,
		    element_type,
		    field_units,
		    element_validation_type
		FROM #fields
		"
	
	SET @sql = REPLACE(REPLACE(@sql, 'study_db', @destination_name), 'dl_db', @datalake_database)
	EXECUTE sp_executesql @sql

	SET @sql = "
		UPDATE f
		SET field_id = rf.id
		FROM #fields f
		JOIN study_db.dbo.meta__redcap_field rf
		    ON rf.meta_form_section_id = f.form_section_id
		    AND rf.ordinal = f.field_order
		    AND rf.name = f.field_name
		"
	
	SET @sql = REPLACE(REPLACE(@sql, 'study_db', @destination_name), 'dl_db', @datalake_database)
	EXECUTE sp_executesql @sql

	SET @sql = "
		INSERT INTO study_db.dbo.meta__redcap_field_enum (meta_field_id, value, name)
		SELECT
		    f.field_id,
		    CONVERT(INT, TRIM(LEFT(value, PATINDEX('%,%', value + ',') - 1))),
		    TRIM(RIGHT(value, LEN(value) - PATINDEX('%,%', value + ',')))
		FROM #fields f
		CROSS APPLY STRING_SPLIT(REPLACE(f.element_enum, '\n', '|'), '|')
		WHERE LEN(TRIM(COALESCE(f.element_enum, ''))) > 0
		    AND element_type <> 'calc'
		"
	
	SET @sql = REPLACE(REPLACE(@sql, 'study_db', @destination_name), 'dl_db', @datalake_database)
	EXECUTE sp_executesql @sql


	DROP TABLE #fields;

	FETCH NEXT FROM db_cursor INTO @study_name, @datalake_database
END 

CLOSE db_cursor
DEALLOCATE db_cursor

SET QUOTED_IDENTIFIER ON

