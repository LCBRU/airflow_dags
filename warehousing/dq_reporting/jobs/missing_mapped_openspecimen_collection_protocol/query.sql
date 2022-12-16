SELECT
	ccsm.case_type_id,
	ccsm.cfg_study_id
FROM warehouse_config.dbo.cfg_civicrm_study_mapping ccsm
WHERE ccsm.case_type_id NOT IN (
	SELECT cc.case_type_id
	FROM warehouse_central.dbo.civicrm__case cc
)
;
