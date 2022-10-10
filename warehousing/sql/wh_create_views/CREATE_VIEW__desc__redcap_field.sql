CREATE OR ALTER VIEW desc__redcap_field AS
SELECT
	mrf2.id AS meta__redcap_field_id,
	mri.datalake_database,
	mrp.redcap_project_id,
	mrp.name AS project_name,
	mrf.name AS form_name,
	mrfs.name AS form_section_name,
	mrf2.name AS field_name,
	mrf2.ordinal AS field_ordinal,
	mrdt.element_type,
	mrdt.element_validation_type
FROM warehouse_central.dbo.meta__redcap_instance mri 
JOIN warehouse_central.dbo.meta__redcap_project mrp 
	ON mrp.meta__redcap_instance_id = mri.id 
JOIN warehouse_central.dbo.meta__redcap_form mrf 
	ON mrf.meta__redcap_project_id = mrp.id
JOIN warehouse_central.dbo.meta__redcap_form_section mrfs 
	ON mrfs.meta__redcap_form_id = mrf.id
JOIN warehouse_central.dbo.meta__redcap_field mrf2 
	ON mrf2.meta__redcap_form_section_id = mrfs.id
JOIN warehouse_central.dbo.meta__redcap_data_type mrdt
	ON mrdt.id = mrf2.meta__redcap_data_type_id
;