IF OBJECT_ID(N'etl_errors') IS NULL
BEGIN
    IF OBJECT_ID('etl_errors') IS NULL
        CREATE TABLE etl_errors (
            id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
            run_id NVARCHAR(100) NOT NULL INDEX ix_etl_errors__run_id NONCLUSTERED,
            created_datetime DATETIME NOT NULL DEFAULT (GETDATE()) INDEX ix_etl_errors__created_datetime NONCLUSTERED,
            title NVARCHAR(200) NOT NULL INDEX ix_etl_errors__title NONCLUSTERED,
            error NVARCHAR(MAX) NOT NULL,
        )
    ;
END
