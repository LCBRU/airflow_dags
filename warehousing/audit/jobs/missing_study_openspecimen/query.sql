
SET QUOTED_IDENTIFIER OFF;
	
CREATE TABLE #stats (
    study_wh_database NVARCHAR(100) NOT NULL,
    collection_protocol_id INT NOT NULL
);

EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'wh_study_%'
BEGIN 
	IF EXISTS(
		SELECT 1 FROM ?.INFORMATION_SCHEMA.TABLES t
		WHERE t.TABLE_NAME = 'desc__openspecimen')
    BEGIN
		INSERT INTO #stats (study_wh_database, collection_protocol_id)
		SELECT DISTINCT
			'?',
			collection_protocol_identifier
		FROM ?.dbo.desc__openspecimen
    END
END"

SELECT
	dbo.study_database_name(cs.name) study_database,
	cosm.collection_protocol_id
FROM warehouse_config.dbo.cfg_openspecimen_study_mapping cosm
JOIN warehouse_config.dbo.cfg_study cs
	ON cs.id = cosm.cfg_study_id
LEFT JOIN #stats s
	ON s.collection_protocol_id = cosm.collection_protocol_id 
	AND s.study_wh_database = dbo.study_database_name(cs.name)
;

DROP TABLE #stats;
