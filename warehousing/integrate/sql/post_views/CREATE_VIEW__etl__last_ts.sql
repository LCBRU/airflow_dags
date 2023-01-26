CREATE OR ALTER VIEW etl__last_ts AS
SELECT (
	SELECT TOP 1 r.dag_run_ts
	FROM warehouse_config.dbo.etl_run r
	ORDER BY r.created_datetime DESC) last_ts,
	(
	SELECT r.dag_run_ts
	FROM warehouse_config.dbo.etl_run r
	ORDER BY r.created_datetime DESC
	OFFSET 1 ROWS
	FETCH NEXT 1 ROWS ONLY) prev_ts
;