SET NOCOUNT ON;
DECLARE @OPENQUERY NVARCHAR(MAX)
DECLARE @SQL NVARCHAR(MAX)
DECLARE @source_database VARCHAR(255)

SELECT @source_database = %(source_database)s

SELECT @SQL = '
    SELECT s.table_name, s.index_name, s.column_name, s.collation, c.data_type, c.character_maximum_length
    FROM INFORMATION_SCHEMA.STATISTICS s
    JOIN INFORMATION_SCHEMA.TABLES t
        ON t.table_schema = s.table_schema
        AND t.table_name = s.table_name
        AND t.table_type = "BASE TABLE"
    JOIN INFORMATION_SCHEMA.COLUMNS c
        ON c.table_schema = s.table_schema
        AND c.table_name = s.table_name
        AND c.column_name = s.column_name
    WHERE s.TABLE_SCHEMA = "' + @source_database + '"
    ORDER BY s.table_name, s.index_name, s.seq_in_index
    '

SELECT @OPENQUERY = '
    SELECT *
    FROM OpenQuery(db01, ''' + @SQL + ''')'

EXEC sp_executesql @OPENQUERY
