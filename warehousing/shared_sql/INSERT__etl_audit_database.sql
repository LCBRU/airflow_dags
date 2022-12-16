IF NOT EXISTS(SELECT 1 FROM warehouse_config.dbo.etl_audit_database WHERE name = DB_NAME())
BEGIN
    INSERT INTO warehouse_config.dbo.etl_audit_database (name)
    VALUES (DB_NAME());
END
