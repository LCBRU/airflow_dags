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
    UPDATE _etl_tables
    SET last_copied = GETDATE(),
        last_copied_updated = last_updated
    WHERE name = @table_name

    FETCH NEXT FROM TABLE_CURSOR INTO @table_name
END
CLOSE TABLE_CURSOR
DEALLOCATE TABLE_CURSOR
