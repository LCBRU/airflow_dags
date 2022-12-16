SELECT dbo.study_database_name(name)
FROM warehouse_config.dbo.cfg_study

EXCEPT

SELECT name
FROM sys.databases
;
