CREATE OR ALTER VIEW desc__cfg_redcap_mapping AS
SELECT
	rm.id,
	rm.cfg_study_id,
	cs.name study_name,
	rm.cfg_redcap_instance_id,
	cri.datalake_database,
	rm.redcap_project_id
FROM cfg_redcap_mapping rm
LEFT JOIN cfg_study cs
	ON cs.id = rm.cfg_study_id
LEFT JOIN cfg_redcap_instance cri
	ON cri.id = rm.cfg_redcap_instance_id
;
