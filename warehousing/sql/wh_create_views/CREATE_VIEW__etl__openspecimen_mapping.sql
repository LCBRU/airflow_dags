CREATE OR ALTER VIEW [dbo].[etl__openspecimen_mapping] AS
SELECT
	cosm.collection_protocol_id,
	cosm.study_id,
	s.name AS study_name,
    dbo.study_database_name(s.name) AS study_database,
	ocp.title AS collection_protocol_title
FROM cfg_openspecimen_study_mapping cosm
JOIN datalake_openspecimen.dbo.catissue_collection_protocol ocp
	ON ocp.identifier = cosm.collection_protocol_id
JOIN datalake_identity.dbo.study s
    ON s.id = cosm.study_id
;