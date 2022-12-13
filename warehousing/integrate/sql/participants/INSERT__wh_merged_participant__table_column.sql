SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX)
DECLARE @cfg_participant_identifier_type_id INT
DECLARE @cfg_participant_source_id INT
DECLARE @table_name VARCHAR(255)
DECLARE @identifier_column_name VARCHAR(255)
DECLARE @source_identifier_column_name VARCHAR(255)

DECLARE TABLE_CURSOR CURSOR
    LOCAL STATIC READ_ONLY FORWARD_ONLY
FOR 
SELECT cfg_participant_identifier_type_id, cfg_participant_source_id, table_name, identifier_column_name, source_identifier_column_name  
FROM warehouse_config.dbo.cfg_participant_identifier_table_column

OPEN TABLE_CURSOR
FETCH NEXT FROM TABLE_CURSOR INTO @cfg_participant_identifier_type_id, @cfg_participant_source_id, @table_name, @identifier_column_name, @source_identifier_column_name
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @SQL = '
		INSERT INTO wh_merged_participant (cfg_participant_identifier_type_id, cfg_participant_source_id, identifier, source_identifier)
		SELECT '
        + CONVERT(VARCHAR(50), @cfg_participant_identifier_type_id)
        + ', ' +  CONVERT(VARCHAR(50), @cfg_participant_source_id)
        + ', ' + QUOTENAME(@identifier_column_name)
        + ', ' + QUOTENAME(@source_identifier_column_name)
        + ' FROM ' + QUOTENAME(@table_name) + ''
        + ' WHERE LEN(TRIM(COALESCE(CONVERT(VARCHAR(100), ' + QUOTENAME(@identifier_column_name) + '), ''''))) > 0'
        + '    AND LEN(TRIM(COALESCE(CONVERT(VARCHAR(100), ' + QUOTENAME(@source_identifier_column_name) + '), ''''))) > 0'
        ;

    print(@SQL)
    EXEC sp_executesql @SQL;

	FETCH NEXT FROM TABLE_CURSOR INTO @cfg_participant_identifier_type_id, @cfg_participant_source_id, @table_name, @identifier_column_name, @source_identifier_column_name
END
CLOSE TABLE_CURSOR
DEALLOCATE TABLE_CURSOR
