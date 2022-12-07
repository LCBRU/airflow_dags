SELECT study_database
FROM etl__civicrm_mapping

EXCEPT

SELECT name
FROM sys.databases
;
