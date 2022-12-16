IF OBJECT_ID(N'etl_audit_table') IS NULL
BEGIN
    CREATE TABLE etl_audit_table (
        id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        name NVARCHAR(500) NOT NULL INDEX ix_etl_audit_table__name NONCLUSTERED,
    )
    ;
END
