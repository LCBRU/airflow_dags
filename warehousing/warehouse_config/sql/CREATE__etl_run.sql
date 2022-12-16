IF OBJECT_ID(N'etl_run') IS NULL
BEGIN
    CREATE TABLE etl_run (
        id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        dag_run_ts NVARCHAR(100) NOT NULL INDEX ix_etl_run__dag_run_ts NONCLUSTERED,
        created_datetime DATETIME NOT NULL DEFAULT (GETDATE()) INDEX ix_etl_run__created_datetime NONCLUSTERED,
    )
    ;
END
