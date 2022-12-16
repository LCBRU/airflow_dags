
SET QUOTED_IDENTIFIER OFF;
	
CREATE TABLE #stats (
    study_wh_database NVARCHAR(100) NOT NULL,
    case_type_id INT NOT NULL,
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
		SET @SQL = 'INSERT INTO #stats (study_wh_database, case_type_id, contact_id)
		SELECT DISTINCT
			''?'',
			case_type_id,
			contact_id
		FROM ?.dbo.civicrm_case cc
		WHERE cc.contact_id NOT IN (
			SELECT id
			FROM ?.dbo.civicrm_contact
		)';

		EXEC sp_executesql @SQL;
    END
END"

SELECT
	study_wh_database,
	case_type_id,
	contact_id
FROM #stats
;

DROP TABLE #stats;
