
SET QUOTED_IDENTIFIER OFF;
	
CREATE TABLE #stats (
    study_wh_database NVARCHAR(100) NOT NULL,
    contact_id INT NOT NULL
);

EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'wh_study_%'
BEGIN 
	IF EXISTS(
		SELECT 1 FROM ?.INFORMATION_SCHEMA.TABLES t
		WHERE t.TABLE_NAME = 'civicrm_case')
    BEGIN
		DECLARE @SQL NVARCHAR(MAX);
		SET @SQL = 'INSERT INTO #stats (study_wh_database, contact_id)
		SELECT DISTINCT
			''?'',
			id
		FROM ?.dbo.civicrm_contact cc
		WHERE cc.id NOT IN (
			SELECT DISTINCT contact_id
			FROM ?.dbo.civicrm_case
		)';

		EXEC sp_executesql @SQL;
    END
END"

SELECT
	study_wh_database,
	contact_id
FROM #stats
;

DROP TABLE #stats;
