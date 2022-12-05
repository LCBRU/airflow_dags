SELECT dbo.study_database_name(study_name) missing_study_data_warehouse
FROM audit__manual_expected me

EXCEPT

SELECT name  
FROM sys.databases
;