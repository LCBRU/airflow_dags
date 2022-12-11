CREATE OR ALTER VIEW [dbo].[etl__civicrm_custom] AS
SELECT
	ccg.id AS custom_group_id,
	ccg.name,
	CONVERT(INT, REPLACE(ccg.extends_entity_column_value, CHAR(1), '')) AS case_type_id,
	ccg.table_name,
	LEFT(ccg.table_name, LEN(ccg.table_name) - CHARINDEX('_', REVERSE(ccg.table_name))) AS warehouse_table_name
FROM datalake_civicrm.dbo.civicrm_custom_group ccg
WHERE extends = 'Case'
;
