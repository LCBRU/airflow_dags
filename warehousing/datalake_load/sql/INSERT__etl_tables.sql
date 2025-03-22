DECLARE @source_database VARCHAR(255)
DECLARE @SQL NVARCHAR(MAX)
DECLARE @OPENQUERY NVARCHAR(MAX)

SELECT @source_database = %(source_database)s

IF OBJECT_ID(N'dbo.__etl_tables', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dbo.__etl_tables
    END;

IF EXISTS(SELECT 1 FROM _etl_tables WHERE name = '_brc__tables')
  BEGIN
    SELECT @SQL = '
        SELECT table_name, update_time
        FROM ' + @source_database + '._brc__tables
        ;
        '      
  END
ELSE
  BEGIN
    SELECT @SQL = '
        SELECT table_name, COALESCE(update_time, create_time) AS update_time
        FROM information_schema.tables
        WHERE table_type = "BASE TABLE"
            AND table_schema = "' + @source_database + '"
        ;
        '      
  END

SELECT @OPENQUERY = '
    SELECT table_name, update_time
    INTO dbo.[__etl_tables]
    FROM OpenQuery(db02, ''' + @SQL + ''')'

EXEC sp_executesql @OPENQUERY

UPDATE _etl_tables
SET extant = 0
WHERE name COLLATE Latin1_General_CI_AS NOT IN (SELECT table_name COLLATE Latin1_General_CI_AS FROM __etl_tables)
;

UPDATE et
SET extant = 1,
    last_updated = update_time
FROM _etl_tables et
JOIN __etl_tables t
    ON t.table_name COLLATE Latin1_General_CI_AS = et.name COLLATE Latin1_General_CI_AS
;

DELETE FROM __etl_tables
WHERE table_name COLLATE Latin1_General_CI_AS IN (SELECT name COLLATE Latin1_General_CI_AS FROM _etl_tables)

INSERT INTO _etl_tables(name, last_copied, last_updated, extant, exclude)
SELECT table_name, NULL, update_time, 1, 0
FROM __etl_tables

-- DROP TABLE __etl_tables;
