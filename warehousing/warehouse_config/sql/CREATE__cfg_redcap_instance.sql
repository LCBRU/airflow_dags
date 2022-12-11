IF OBJECT_ID(N'cfg_redcap_instance') IS NULL
BEGIN
	CREATE TABLE cfg_redcap_instance (
		id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
		source_database nvarchar(500),
		datalake_database nvarchar(500),
		INDEX idx__cfg_redcap_instance__source_database (source_database),
		INDEX idx__cfg_redcap_instance__datalake_database (datalake_database),
	);

	SET IDENTITY_INSERT cfg_redcap_instance ON;

	INSERT INTO cfg_redcap_instance (id, source_database, datalake_database) VALUES
		(1, N'redcap6170_briccs', 'datalake_redcap_uhl'),
		(2, N'redcap6170_briccsext', 'datalake_redcap_n3'),
		(3, N'redcap_genvasc', 'datalake_redcap_genvasc'),
		(4, N'uol_crf_redcap', 'datalake_redcap_uol'),
		(5, N'uol_survey_redcap', 'datalake_redcap_internet'),
		(6, N'uol_easyas_redcap', 'datalake_redcap_easyas'),
		(7, N'redcap_national', 'datalake_redcap_national')
	;

	SET IDENTITY_INSERT cfg_redcap_instance OFF;
END