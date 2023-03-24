SELECT crp.*
FROM (
	SELECT
			'datalake_redcap_easyas' AS datalake_database,
			project_id,
			project_name,
			app_title,
			status
	FROM datalake_redcap_easyas.dbo.redcap_projects rp
	WHERE rp.project_name NOT LIKE 'redcap_demo_%'
			AND rp.status = 1

	UNION ALL

	SELECT
			'datalake_redcap_internet' AS datalake_database,
			project_id,
			project_name,
			app_title,
			status
	FROM datalake_redcap_internet.dbo.redcap_projects rp
	WHERE rp.project_name NOT LIKE 'redcap_demo_%'
			AND rp.status = 1

	UNION ALL

	SELECT
			'datalake_redcap_n3' AS datalake_database,
			project_id,
			project_name,
			app_title,
			status
	FROM datalake_redcap_n3.dbo.redcap_projects rp
	WHERE rp.project_name NOT LIKE 'redcap_demo_%'
			AND rp.status = 1

	UNION ALL

	SELECT
			'datalake_redcap_national' AS datalake_database,
			project_id,
			project_name,
			app_title,
			status
	FROM datalake_redcap_national.dbo.redcap_projects rp
	WHERE rp.project_name NOT LIKE 'redcap_demo_%'
			AND rp.status = 1

	UNION ALL

	SELECT
			'datalake_redcap_uhl' AS datalake_database,
			project_id,
			project_name,
			app_title,
			status
	FROM datalake_redcap_uhl.dbo.redcap_projects rp
	WHERE rp.project_name NOT LIKE 'redcap_demo_%'
			AND rp.status = 1

	UNION ALL

	SELECT
			'datalake_redcap_uol' AS datalake_database,
			project_id,
			project_name,
			app_title,
			status
	FROM datalake_redcap_uol.dbo.redcap_projects rp
	WHERE rp.project_name NOT LIKE 'redcap_demo_%'
		AND rp.status = 1
) crp
LEFT JOIN warehouse_config.dbo.cfg_redcap_instance cri
	ON cri.datalake_database = crp.datalake_database
LEFT JOIN warehouse_config.dbo.cfg_redcap_mapping crm
	ON crm.redcap_project_id = crp.project_id
WHERE crm.id IS NULL
	AND crp.status = 1
--    -- 0 = Development
--    -- 1 = Online
--    -- 2 = ?
--    -- 3 = Archived
    AND crm.cfg_study_id > 0
;