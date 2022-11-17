CREATE VIEW civicrm_contact AS
SELECT *
FROM warehouse_central.dbo.civicrm__contact cc
WHERE cc.id IN (
	SELECT contact_id
	FROM warehouse_central.dbo.civicrm__case ccase
	JOIN warehouse_central.dbo.etl__civicrm_mapping ecm
		ON ecm.case_type_id = ccase.case_type_id
	WHERE ecm.study_name = %(study_name)s
)
;
