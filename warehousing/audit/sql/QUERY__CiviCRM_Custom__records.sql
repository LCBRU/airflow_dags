SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX)
DECLARE @table_name VARCHAR(255)

DECLARE TABLE_CURSOR CURSOR
    LOCAL STATIC READ_ONLY FORWARD_ONLY
FOR
SELECT warehouse_table_name
FROM etl__civicrm_custom
IF NOT EXISTS(SELECT 1 FROM warehouse_config.dbo.etl_audit_database WHERE name = DB_NAME())
BEGIN
    INSERT INTO warehouse_config.dbo.etl_audit_database (name)
    VALUES (DB_NAME());
END

OPEN TABLE_CURSOR
FETCH NEXT FROM TABLE_CURSOR INTO @table_name
WHILE @@FETCH_STATUS = 0
BEGIN

	IF NOT EXISTS(SELECT 1 FROM warehouse_config.dbo.etl_audit_table WHERE name = @table_name)
	BEGIN
		INSERT INTO warehouse_config.dbo.etl_audit_table (name)
		VALUES (@table_name);
	END

    SELECT @SQL = '
INSERT INTO warehouse_config.dbo.etl_audit (run_id, group_id, group_type_id, cfg_participant_source_id, cfg_study_id, database_id, table_id, count_type_id, count)
SELECT
	er.id,
	'''' AS group_id,
	gt.id AS group_type_id,
	cps.id AS cfg_participant_source_id,
	NULL AS cfg_study_id,
	ad.id AS database_id,
	atab.id AS table_id,
	ct.id AS count_type_id,
	COUNT(*) records
FROM warehouse_central.dbo.' + @table_name + ' t
JOIN warehouse_config.dbo.etl_run er
	ON er.dag_run_ts = ''{{ ts }}''
JOIN warehouse_config.dbo.etl_audit_database ad
	ON ad.name = DB_NAME()
JOIN warehouse_config.dbo.etl_audit_table atab
	ON atab.name = ''' +  @table_name + '''
JOIN warehouse_config.dbo.cfg_participant_source cps
	ON cps.name = ''CiviCRM Case''
JOIN warehouse_config.dbo.etl_audit_group_type gt
	ON gt.name = ''table''
JOIN warehouse_config.dbo.etl_audit_group_type ct
	ON ct.name = ''record''
GROUP BY
	cps.id,
	gt.id,
	ct.id,
	er.id,
	ad.id,
	atab.id
	';

    IF OBJECT_ID(N'dbo.[' + @table_name + ']', N'U') IS NOT NULL
        BEGIN
            EXEC sp_executesql @SQL;
        END;

    FETCH NEXT FROM TABLE_CURSOR INTO @table_name
END
CLOSE TABLE_CURSOR
DEALLOCATE TABLE_CURSOR
