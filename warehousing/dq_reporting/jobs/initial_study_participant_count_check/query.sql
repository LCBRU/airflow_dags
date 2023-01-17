
SET QUOTED_IDENTIFIER OFF;
	
CREATE TABLE #stats (
    study_wh_database NVARCHAR(100) NOT NULL,
    redcap_project_id INT NOT NULL,
    participant_count INT NOT NULL
);

EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'wh_study_%'
BEGIN 
	IF EXISTS(
		SELECT 1 FROM ?.INFORMATION_SCHEMA.TABLES t
		WHERE t.TABLE_NAME = 'desc__redcap_data')
    BEGIN
		INSERT INTO #stats (study_wh_database, redcap_project_id, participant_count)
		SELECT
			'?',
			redcap_project_id,
			COUNT(DISTINCT redcap_participant_id)
		FROM ?.dbo.desc__redcap_data
		GROUP BY redcap_project_id
    END
END"

SELECT
	ame.study_name,
	ame.datalake_database,
	ame.project_id,
	ame.project_name,
	ame.participant_count AS expected_participant_count,
	COALESCE(s.participant_count, 0) AS actual_participant_count
FROM warehouse_central.dbo.audit__manual_expected ame
LEFT JOIN #stats s
	ON s.redcap_project_id = ame.project_id 
	AND s.study_wh_database = dbo.study_database_name(ame.study_name)
WHERE ame.participant_count <> COALESCE(s.participant_count, 0)
;

DROP TABLE #stats;


-- SELECT *
-- FROM audit__manual_expected ame
-- LEFT JOIN warehouse_config.dbo.desc__etl_audit dea
-- 	ON dea.study_name = ame.study_name
-- 	AND dea.participant_source_name = 'redcap'
-- 	AND dea.group_type_name = 'REDCap Project'
-- 	AND dea.group_id = ame.project_id
-- 	AND dea.database_name = dbo.study_database_name(ame.study_name)
--  AND dea.table_name = 'desc__redcap_data'
-- 	AND dea.count_type_name = 'REDCap Participant'
-- WHERE ame.source_system = 'redcap'
-- 	AND ame.participant_count <> COALESCE(dea.count, 0)
