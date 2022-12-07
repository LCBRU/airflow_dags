SELECT study_database
FROM etl__openspecimen_mapping

EXCEPT

SELECT name
FROM sys.databases
;
