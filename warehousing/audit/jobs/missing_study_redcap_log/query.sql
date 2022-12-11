
SET QUOTED_IDENTIFIER OFF;
	
CREATE TABLE #stats (
    study_wh_database NVARCHAR(100) NOT NULL,
    redcap_project_id INT NOT NULL
);

EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'wh_study_%'
BEGIN 
	IF EXISTS(
		SELECT 1 FROM ?.INFORMATION_SCHEMA.TABLES t
		WHERE t.TABLE_NAME = 'desc__redcap_log')
    BEGIN
		INSERT INTO #stats (study_wh_database, redcap_project_id)
		SELECT DISTINCT
			'?',
			redcap_project_id
		FROM ?.dbo.desc__redcap_log
    END
END"

SELECT
	dbo.study_database_name(cs.name) study_database,
	cri.datalake_database,
	crm.redcap_project_id
FROM cfg_redcap_mapping crm
JOIN cfg_study cs
	ON cs.id = crm.cfg_study_id
JOIN cfg_redcap_instance cri
	ON cri.id = crm.cfg_redcap_instance_id
LEFT JOIN #stats s
	ON s.redcap_project_id = crm.redcap_project_id 
	AND s.study_wh_database = dbo.study_database_name(cs.name)
;

DROP TABLE #stats;
