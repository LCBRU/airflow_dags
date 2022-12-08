CREATE OR ALTER VIEW [dbo].[etl__openspecimen_mapping] AS
SELECT
	cosm.collection_protocol_id,
	cosm.study_id,
	s.name AS study_name,
    dbo.study_database_name(s.name) AS study_database
FROM cfg_openspecimen_study_mapping cosm
JOIN cfg_study s
	ON s.id = cosm.study_id
;