CREATE OR ALTER VIEW [dbo].[etl__redcap_project_mapping] AS
SELECT
        s.name AS study_name,
        es.name AS redcap_project_name,
        rp.project_id AS redcap_project_id,
        ri.database_name AS source_database_name,
	wcri.datalake_database
FROM datalake_identity.dbo.participant_import_definition pid
JOIN datalake_identity.dbo.study s
        ON s.id = pid.study_id
JOIN datalake_identity.dbo.ecrf_source es
        ON es.id = pid.ecrf_source_id
JOIN datalake_identity.dbo.redcap_project rp
        ON rp.id = es.id
JOIN datalake_identity.dbo.redcap_instance ri
        ON ri.id = rp.redcap_instance_id
JOIN meta__redcap_instance wcri
	ON wcri.source_database = ri.database_name
JOIN merged__redcap_project crp
	ON crp.datalake_database_name = wcri.datalake_database
	AND crp.project_id = rp.project_id
	AND crp.status = 1
;