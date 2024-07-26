SET NOCOUNT ON;
DECLARE @OPENQUERY NVARCHAR(MAX)
DECLARE @SQL NVARCHAR(MAX)
DECLARE @table_name VARCHAR(255)
DECLARE @source_database VARCHAR(255)

SELECT @source_database = %(source_database)s

DECLARE TABLE_CURSOR CURSOR
    LOCAL STATIC READ_ONLY FORWARD_ONLY
FOR 
SELECT name 
FROM _etl_tables
WHERE extant = 1
    AND exclude = 0
    AND (last_copied_updated IS NULL OR last_copied_updated < last_updated)

OPEN TABLE_CURSOR
FETCH NEXT FROM TABLE_CURSOR INTO @table_name
WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT 'Dropping ' + @table_name

    SELECT @SQL = 'DROP TABLE dbo.' + QUOTENAME(@table_name) + '';

    IF OBJECT_ID(N'dbo.[' + @table_name + ']', N'U') IS NOT NULL
        BEGIN
            EXEC sp_executesql @SQL;
        END;

    PRINT 'Copying ' + @table_name

    SELECT @SQL = '
        SELECT *
        FROM `' + @source_database + '`.`' + @table_name + '`'

    SELECT @OPENQUERY = '
        SELECT *
        INTO [' + @table_name + ']
        FROM OpenQuery(db02, ''' + @SQL + ''')'

    EXEC sp_executesql @OPENQUERY

    FETCH NEXT FROM TABLE_CURSOR INTO @table_name
END
CLOSE TABLE_CURSOR
DEALLOCATE TABLE_CURSOR
