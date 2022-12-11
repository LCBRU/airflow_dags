SELECT dbo.study_database_name(name)
FROM cfg_study

EXCEPT

SELECT name
FROM sys.databases
;
