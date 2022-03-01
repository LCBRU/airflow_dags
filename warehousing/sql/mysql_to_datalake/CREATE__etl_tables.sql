IF OBJECT_ID(N'dbo._etl_tables', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo._etl_tables (
            name VARCHAR(255) not null,
            last_copied DATETIME NULL,
            last_updated DATETIME NULL,
            extant BIT,
            exclude BIT
        );
    END;