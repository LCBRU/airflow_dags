SET NOCOUNT ON;

DECLARE @SQL NVARCHAR(MAX)
DECLARE @db_name VARCHAR(255)

SELECT @db_name = %(db_name)s

IF DB_ID(@db_name) IS NULL
    BEGIN
        PRINT 'Creating database ' + @db_name

        SELECT @SQL = 'CREATE DATABASE ' + QUOTENAME(@db_name) + '';
        EXEC sp_executesql @SQL

        SELECT @SQL = 'ALTER DATABASE ' + QUOTENAME(@db_name) + ' SET RECOVERY SIMPLE';
        EXEC sp_executesql @SQL

        SELECT @SQL = 'ALTER DATABASE ' + QUOTENAME(@db_name) + ' SET MULTI_USER';
        EXEC sp_executesql @SQL
    END;