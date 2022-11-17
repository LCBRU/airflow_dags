CREATE VIEW civicrm_case AS
SELECT *
FROM warehouse_central.dbo.civicrm__case cc
WHERE cc.case_type_id IN (
	SELECT case_type_id
	FROM warehouse_central.dbo.etl__civicrm_mapping ecm
	WHERE ecm.study_name = %(study_name)s
)
;
