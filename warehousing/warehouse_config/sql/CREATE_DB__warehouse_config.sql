SET NOCOUNT ON;

IF DB_ID('warehouse_config') IS NULL
    BEGIN
        CREATE DATABASE warehouse_config
    END;
