IF NOT EXISTS(SELECT 1 FROM warehouse_config.dbo.etl_audit_table WHERE name = '{{ param.table_name }}')
BEGIN
    INSERT INTO warehouse_config.dbo.etl_audit_table (name)
    VALUES ('{{ param.table_name }}');
END
