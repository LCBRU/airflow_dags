IF OBJECT_ID(N'etl_error') IS NULL
BEGIN
    CREATE TABLE etl_error (
        id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        run_id INT NOT NULL INDEX ix_etl_errors__run_id NONCLUSTERED,
        created_datetime DATETIME NOT NULL DEFAULT (GETDATE()) INDEX ix_etl_errors__created_datetime NONCLUSTERED,
        title NVARCHAR(200) NOT NULL INDEX ix_etl_errors__title NONCLUSTERED,
        error NVARCHAR(MAX) NOT NULL,
        FOREIGN KEY (run_id) REFERENCES etl_run(id),
    )
    ;
END
