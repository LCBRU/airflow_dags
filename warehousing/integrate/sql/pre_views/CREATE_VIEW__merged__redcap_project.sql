CREATE OR ALTER VIEW merged__redcap_project AS
SELECT
        'datalake_redcap_easyas' AS datalake_database,
        project_id,
        project_name,
        app_title,
        status
FROM datalake_redcap_easyas.dbo.redcap_projects rp
WHERE rp.project_name NOT LIKE 'redcap_demo_%'

UNION ALL

SELECT
        'datalake_redcap_internet' AS datalake_database,
        project_id,
        project_name,
        app_title,
        status
FROM datalake_redcap_internet.dbo.redcap_projects rp
WHERE rp.project_name NOT LIKE 'redcap_demo_%'

UNION ALL

SELECT
        'datalake_redcap_n3' AS datalake_database,
        project_id,
        project_name,
        app_title,
        status
FROM datalake_redcap_n3.dbo.redcap_projects rp
WHERE rp.project_name NOT LIKE 'redcap_demo_%'

UNION ALL

SELECT
        'datalake_redcap_national' AS datalake_database,
        project_id,
        project_name,
        app_title,
        status
FROM datalake_redcap_national.dbo.redcap_projects rp
WHERE rp.project_name NOT LIKE 'redcap_demo_%'

UNION ALL

SELECT
        'datalake_redcap_uhl' AS datalake_database,
        project_id,
        project_name,
        app_title,
        status
FROM datalake_redcap_uhl.dbo.redcap_projects rp
WHERE rp.project_name NOT LIKE 'redcap_demo_%'

UNION ALL

SELECT
        'datalake_redcap_uol' AS datalake_database,
        project_id,
        project_name,
        app_title,
        status
FROM datalake_redcap_uol.dbo.redcap_projects rp
WHERE rp.project_name NOT LIKE 'redcap_demo_%'
;