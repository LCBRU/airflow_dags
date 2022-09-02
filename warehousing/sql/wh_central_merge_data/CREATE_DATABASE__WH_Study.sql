DECLARE @name NVARCHAR(500) 
DECLARE @sql NVARCHAR(MAX) 

DECLARE db_cursor CURSOR FOR 
SELECT DISTINCT study_name
FROM datalake_redcap_project_mappings

OPEN db_cursor  
FETCH NEXT FROM db_cursor INTO @name  

WHILE @@FETCH_STATUS = 0  
BEGIN
	SET @sql = N'CREATE DATABASE [wh_study_' + @name + ']'
	EXECUTE sp_executesql @sql
	
	SET @sql = N'ALTER DATABASE [wh_study_' + @name + '] SET RECOVERY SIMPLE'
	EXECUTE sp_executesql @sql
	
	FETCH NEXT FROM db_cursor INTO @name
END 

CLOSE db_cursor  
DEALLOCATE db_cursor 


EXEC sp_MSforeachdb
@command1='IF ''?'' LIKE ''wh_study_%''
BEGIN
	IF OBJECT_ID(N''[?].dbo.redcap_form'') IS NOT NULL
		DROP TABLE [?].dbo.redcap_form

	CREATE TABLE [?].dbo.redcap_form (
		id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
		name NVARCHAR(100) NOT NULL UNIQUE
	);

	IF OBJECT_ID(N''[?].dbo.redcap_form_section'') IS NOT NULL
		DROP TABLE [?].dbo.redcap_form_section

	CREATE TABLE [?].dbo.redcap_form_section (
		id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
		form_id INT NOT NULL,
		name NVARCHAR(100) NOT NULL,
		INDEX idx__redcap_form_section__name (name),
		FOREIGN KEY (form_id) REFERENCES redcap_form(id),
		UNIQUE (form_id, name)
	);

	IF OBJECT_ID(N''[?].dbo.redcap_field'') IS NOT NULL
		DROP TABLE [?].dbo.redcap_field

	CREATE TABLE [?].dbo.redcap_field (
		id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
		form_section_id INT NOT NULL,
		ordinal INT NOT NULL,
		name NVARCHAR(100) NOT NULL,
		label NVARCHAR(MAX) NOT NULL,
		type VARCHAR(50) NOT NULL,
		units VARCHAR(50) NULL,
		validation_type VARCHAR(255) NULL,
		INDEX idx__redcap_field__form_section (form_section_id),
		INDEX idx__redcap_field__ordinal (ordinal),
		INDEX idx__redcap_field__name (name),
		FOREIGN KEY (form_section_id) REFERENCES redcap_form_section(id),
		UNIQUE (form_section_id, name)
	);

	IF OBJECT_ID(N''[?].dbo.redcap_field_enum'') IS NOT NULL
		DROP TABLE [?].dbo.redcap_field_enum

	CREATE TABLE [?].dbo.redcap_field_enum (
		id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
		field_id INT NOT NULL,
		value INT NOT NULL,
		name NVARCHAR(100) NOT NULL,
		INDEX idx__redcap_field_enum__field (field_id),
		INDEX idx__redcap_field_enum__name (name),
		UNIQUE (field_id, value),
		UNIQUE (field_id, name)
	);

END'
