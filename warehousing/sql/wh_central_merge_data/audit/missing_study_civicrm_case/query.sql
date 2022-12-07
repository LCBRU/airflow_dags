
SET QUOTED_IDENTIFIER OFF;
	
CREATE TABLE #stats (
    study_wh_database NVARCHAR(100) NOT NULL,
    case_type_id INT NOT NULL
);

EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'wh_study_%'
BEGIN 
	IF EXISTS(
		SELECT 1 FROM ?.INFORMATION_SCHEMA.TABLES t
		WHERE t.TABLE_NAME = 'civicrm_case')
    BEGIN
		INSERT INTO #stats (study_wh_database, case_type_id)
		SELECT DISTINCT
			'?',
			case_type_id
		FROM ?.dbo.civicrm_case
    END
END"

SELECT
	cm.study_database,
	cm.case_type_id
FROM etl__civicrm_mapping cm
LEFT JOIN #stats s
	ON s.case_type_id = cm.case_type_id 
	AND s.study_wh_database = cm.study_database
;

DROP TABLE #stats;
