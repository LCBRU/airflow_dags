
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
	rpm.study_database,
	rpm.datalake_database,
	rpm.redcap_project_id
FROM etl__redcap_project_mapping rpm
LEFT JOIN #stats s
	ON s.redcap_project_id = rpm.redcap_project_id 
	AND s.study_wh_database = rpm.study_database
;

DROP TABLE #stats;
