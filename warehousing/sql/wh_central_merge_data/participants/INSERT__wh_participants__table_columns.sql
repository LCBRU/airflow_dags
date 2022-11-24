SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX)
DECLARE @identifier_type_id INT
DECLARE @source_type_id INT
DECLARE @table_name VARCHAR(255)
DECLARE @identifier_column_name VARCHAR(255)
DECLARE @source_identifier_column_name VARCHAR(255)

DECLARE TABLE_CURSOR CURSOR
    LOCAL STATIC READ_ONLY FORWARD_ONLY
FOR 
SELECT identifier_type_id, source_type_id, table_name, identifier_column_name, source_identifier_column_name  
FROM warehouse_central.dbo.cfg_wh_participant_identifier_table_columns

OPEN TABLE_CURSOR
FETCH NEXT FROM TABLE_CURSOR INTO @identifier_type_id, @source_type_id, @table_name, @identifier_column_name, @source_identifier_column_name
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @SQL = '
		INSERT INTO wh_participants (identifier_type_id, source_type_id, identifier, source_identifier)
		SELECT '
        + CONVERT(VARCHAR(50), @identifier_type_id)
        + ', ' +  CONVERT(VARCHAR(50), @source_type_id)
        + ', ' + QUOTENAME(@identifier_column_name)
        + ', ' + QUOTENAME(@source_identifier_column_name)
        + ' FROM ' + QUOTENAME(@table_name) + ''
        + ' WHERE LEN(TRIM(COALESCE(CONVERT(VARCHAR(100), ' + QUOTENAME(@identifier_column_name) + '), ''''))) > 0'
        + '    AND LEN(TRIM(COALESCE(CONVERT(VARCHAR(100), ' + QUOTENAME(@source_identifier_column_name) + '), ''''))) > 0'
        ;

    print(@SQL)
    EXEC sp_executesql @SQL;

	FETCH NEXT FROM TABLE_CURSOR INTO @identifier_type_id, @source_type_id, @table_name, @identifier_column_name, @source_identifier_column_name
END
CLOSE TABLE_CURSOR
DEALLOCATE TABLE_CURSOR
