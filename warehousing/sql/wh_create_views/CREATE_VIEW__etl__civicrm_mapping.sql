CREATE OR ALTER VIEW [dbo].[etl__civicrm_mapping] AS
SELECT
	ccsm.case_type_id,
	s.name AS study_name,
    dbo.study_database_name(s.name) AS study_database
FROM cfg_civicrm_study_mapping ccsm
JOIN cfg_study s
    ON s.id = ccsm.study_id
;