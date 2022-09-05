EXEC sp_MSforeachdb @command1='
IF ''?'' LIKE ''wh_study_%''
BEGIN
	IF OBJECT_ID(N''[?].dbo.redcap_field_enum'') IS NOT NULL
		DROP TABLE [?].dbo.redcap_field_enum

	IF OBJECT_ID(N''[?].dbo.redcap_field'') IS NOT NULL
		DROP TABLE [?].dbo.redcap_field

	IF OBJECT_ID(N''[?].dbo.redcap_form_section'') IS NOT NULL
		DROP TABLE [?].dbo.redcap_form_section

	IF OBJECT_ID(N''[?].dbo.redcap_form'') IS NOT NULL
		DROP TABLE [?].dbo.redcap_form

	IF OBJECT_ID(N''[?].dbo.redcap_project'') IS NOT NULL
		DROP TABLE [?].dbo.redcap_project

	IF OBJECT_ID(N''[?].dbo.redcap_instance'') IS NOT NULL
		DROP TABLE [?].dbo.redcap_instance
END'


EXEC sp_MSforeachdb
@command1='IF ''?'' LIKE ''wh_study_%''
BEGIN
	CREATE TABLE [?].dbo.redcap_instance (
		id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
		datalake_database NVARCHAR(100) NOT NULL UNIQUE
	);

	CREATE TABLE [?].dbo.redcap_project (
		id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
		instance_id INT NOT NULL,
		redcap_project_id INT NOT NULL,
		name NVARCHAR(100) NOT NULL,
		INDEX idx__redcap_project__name (name),
		FOREIGN KEY (instance_id) REFERENCES redcap_instance(id),
		UNIQUE (instance_id, redcap_project_id),
		UNIQUE (instance_id, name)
	);

	CREATE TABLE [?].dbo.redcap_form (
		id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
		project_id INT NOT NULL,
		name NVARCHAR(100) NOT NULL,
		INDEX idx__redcap_form__name (name),
		FOREIGN KEY (project_id) REFERENCES redcap_project(id),
		UNIQUE (project_id, name)
	);

	CREATE TABLE [?].dbo.redcap_form_section (
		id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
		form_id INT NOT NULL,
		name NVARCHAR(100) NOT NULL,
		INDEX idx__redcap_form_section__name (name),
		FOREIGN KEY (form_id) REFERENCES redcap_form(id),
		UNIQUE (form_id, name)
	);

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
