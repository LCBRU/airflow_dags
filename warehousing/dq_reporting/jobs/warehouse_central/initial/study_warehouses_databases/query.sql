SELECT dbo.study_database_name(name) missing_study_data_warehouse
FROM warehouse_config.dbo.cfg_study cs 

EXCEPT

SELECT name  
FROM sys.databases
;