
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
	dbo.study_database_name(cs.name) study_database,
	ccsm.case_type_id
FROM warehouse_config.dbo.cfg_civicrm_study_mapping ccsm
JOIN warehouse_config.dbo.cfg_study cs
	ON cs.id = ccsm.cfg_study_id 
LEFT JOIN #stats s
	ON s.case_type_id = ccsm.case_type_id 
	AND s.study_wh_database = dbo.study_database_name(cs.name)
;

DROP TABLE #stats;
