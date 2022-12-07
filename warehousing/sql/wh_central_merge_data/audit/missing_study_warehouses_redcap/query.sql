SELECT study_database
FROM etl__redcap_project_mapping

EXCEPT

SELECT name
FROM sys.databases
;
