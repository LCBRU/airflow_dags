EXEC sp_MSforeachdb @command1='
IF ''?'' LIKE ''wh_study_%''
BEGIN
	IF OBJECT_ID(N''[?].dbo.meta__redcap_field_enum'') IS NOT NULL
		DROP TABLE [?].dbo.meta__redcap_field_enum

	IF OBJECT_ID(N''[?].dbo.meta__redcap_field'') IS NOT NULL
		DROP TABLE [?].dbo.meta__redcap_field

	IF OBJECT_ID(N''[?].dbo.meta__redcap_form_section'') IS NOT NULL
		DROP TABLE [?].dbo.meta__redcap_form_section

	IF OBJECT_ID(N''[?].dbo.meta__redcap_form'') IS NOT NULL
		DROP TABLE [?].dbo.meta__redcap_form

	IF OBJECT_ID(N''[?].dbo.meta__redcap_project'') IS NOT NULL
		DROP TABLE [?].dbo.meta__redcap_project

	IF OBJECT_ID(N''[?].dbo.meta__redcap_instance'') IS NOT NULL
		DROP TABLE [?].dbo.meta__redcap_instance
END'


EXEC sp_MSforeachdb
@command1='IF ''?'' LIKE ''wh_study_%''
BEGIN
	CREATE TABLE [?].dbo.meta__redcap_instance (
		id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
		datalake_database NVARCHAR(100) NOT NULL UNIQUE
	);

	CREATE TABLE [?].dbo.meta__redcap_project (
		id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
		meta_instance_id INT NOT NULL,
		redcap_project_id INT NOT NULL,
		name NVARCHAR(100) NOT NULL,
		INDEX idx__redcap_project__name (name),
		FOREIGN KEY (meta_instance_id) REFERENCES meta__redcap_instance(id),
		UNIQUE (meta_instance_id, redcap_project_id),
		UNIQUE (meta_instance_id, name)
	);

	CREATE TABLE [?].dbo.meta__redcap_form (
		id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
		meta_project_id INT NOT NULL,
		name NVARCHAR(100) NOT NULL,
		INDEX idx__redcap_form__name (name),
		FOREIGN KEY (meta_project_id) REFERENCES meta__redcap_project(id),
		UNIQUE (meta_project_id, name)
	);

	CREATE TABLE [?].dbo.meta__redcap_form_section (
		id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
		meta_form_id INT NOT NULL,
		name NVARCHAR(100) NOT NULL,
		INDEX idx__redcap_form_section__name (name),
		FOREIGN KEY (meta_form_id) REFERENCES meta__redcap_form(id),
		UNIQUE (meta_form_id, name)
	);
END'

EXEC sp_MSforeachdb
@command1='IF ''?'' LIKE ''wh_study_%''
BEGIN
	CREATE TABLE [?].dbo.meta__redcap_field (
		id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
		meta_form_section_id INT NOT NULL,
		ordinal INT NOT NULL,
		name NVARCHAR(100) NOT NULL,
		label NVARCHAR(MAX) NOT NULL,
		type VARCHAR(50) NOT NULL,
		units VARCHAR(50) NULL,
		validation_type VARCHAR(255) NULL,
		INDEX idx__redcap_field__form_section (meta_form_section_id),
		INDEX idx__redcap_field__ordinal (ordinal),
		INDEX idx__redcap_field__name (name),
		FOREIGN KEY (meta_form_section_id) REFERENCES meta__redcap_form_section(id),
		UNIQUE (meta_form_section_id, name)
	);

	CREATE TABLE [?].dbo.meta__redcap_field_enum (
		id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
		meta_field_id INT NOT NULL,
		value INT NOT NULL,
		name NVARCHAR(100) NOT NULL,
		INDEX idx__redcap_field_enum__field (meta_field_id),
		INDEX idx__redcap_field_enum__name (name),
		UNIQUE (meta_field_id, value),
		UNIQUE (meta_field_id, name)
	);
END'
