SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX)
DECLARE @db_name VARCHAR(255)

DECLARE TABLE_CURSOR CURSOR
    LOCAL STATIC READ_ONLY FORWARD_ONLY
FOR
SELECT DISTINCT 'wh_' + TRANSLATE(LOWER(study_name), ' -', '__')
FROM datalake_redcap_project_mappings drpm

OPEN TABLE_CURSOR
FETCH NEXT FROM TABLE_CURSOR INTO @db_name
WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT 'Dropping ' + @db_name

    SELECT @SQL = 'DROP DATABASE ' + QUOTENAME(@db_name) + '';

    IF DB_ID(@db_name) IS NOT NULL
        BEGIN
            EXEC sp_executesql @SQL;
        END;

    PRINT 'Creating database ' + @db_name

    SELECT @SQL = 'CREATE DATABASE ' + QUOTENAME(@db_name) + '';
    EXEC sp_executesql @SQL

    SELECT @SQL = 'ALTER DATABASE ' + QUOTENAME(@db_name) + ' SET RECOVERY SIMPLE';
    EXEC sp_executesql @SQL

    FETCH NEXT FROM TABLE_CURSOR INTO @db_name
END
CLOSE TABLE_CURSOR
DEALLOCATE TABLE_CURSOR
