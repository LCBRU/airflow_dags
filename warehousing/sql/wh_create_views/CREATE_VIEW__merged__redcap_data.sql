CREATE OR ALTER VIEW merged__redcap_data AS
SELECT
        'datalake_redcap_easyas' AS datalake_database, *
FROM datalake_redcap_easyas.dbo.redcap_data

UNION ALL

SELECT
        'datalake_redcap_internet' AS datalake_database, *
FROM datalake_redcap_internet.dbo.redcap_data

UNION ALL

SELECT
        'datalake_redcap_n3' AS datalake_database, *
FROM datalake_redcap_n3.dbo.redcap_data

UNION ALL

SELECT
        'datalake_redcap_national' AS datalake_database, *
FROM datalake_redcap_national.dbo.redcap_data

UNION ALL

SELECT
        'datalake_redcap_uhl' AS datalake_database, *
FROM datalake_redcap_uhl.dbo.redcap_data

UNION ALL

SELECT
        'datalake_redcap_uol' AS datalake_database, *
FROM datalake_redcap_uol.dbo.redcap_data
;