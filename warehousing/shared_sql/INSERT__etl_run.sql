IF NOT EXISTS(SELECT 1 FROM warehouse_config.dbo.etl_run WHERE dag_run_ts = '{{ ts }}')
BEGIN
    INSERT INTO warehouse_config.dbo.etl_run (dag_run_ts)
    VALUES ('{{ ts }}');
END
