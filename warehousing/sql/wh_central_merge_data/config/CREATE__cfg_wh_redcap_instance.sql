CREATE TABLE cfg_wh_redcap_instance (
	id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
	source_database nvarchar(500),
	datalake_database nvarchar(500),
	INDEX idx__cfg_wh_redcap_instance__source_database (source_database),
	INDEX idx__cfg_wh_redcap_instance__datalake_database (datalake_database),
);

-- NHS IDs
INSERT INTO cfg_wh_redcap_instance (source_database, datalake_database) VALUES
	(N'redcap6170_briccs', 'datalake_redcap_uhl'),
	(N'redcap6170_briccsext', 'datalake_redcap_n3'),
	(N'redcap_genvasc', 'datalake_redcap_genvasc'),
	(N'uol_crf_redcap', 'datalake_redcap_uol'),
	(N'uol_survey_redcap', 'datalake_redcap_internet'),
	(N'uol_easyas_redcap', 'datalake_redcap_easyas'),
	(N'redcap_national', 'datalake_redcap_national')
;
