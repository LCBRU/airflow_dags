CREATE OR ALTER VIEW desc__redcap_data AS
SELECT
	rv.id,
	rv.meta__redcap_instance_id,
	mri.datalake_database,
	rv.meta__redcap_project_id,
	mrp.name project_name,
	mrp.redcap_project_id,
	rv.meta__redcap_arm_id,
	mra.redcap_arm_id,
	mra.arm_num,
	mra.name arm_name,
	rv.meta__redcap_event_id,
	mre.redcap_event_id,
	mre.name event_name,
	rv.meta__redcap_form_id,
	mrf.name form_name,
	rv.meta__redcap_form_section_id,
	mrfs.name form_section_name,
	rv.meta__redcap_field_id,
	mrf2.name field_name,
	mrf2.ordinal field_ordinal,
	mrf2.label field_label,
	mrf2.units field_units,
	rv.redcap_participant_id,
	rp.record,
	rv.meta__redcap_field_enum_id,
	mrfe.name field_enum_name,
	rv.meta__redcap_data_type_id,
	rdt.element_type,
	rdt.element_validation_type,
	rv.redcap_file_id,
	rf.stored_name file_name,
	rv.[instance],
	rv.text_value,
	rv.datetime_value,
	rv.date_value,
	rv.time_value,
	rv.int_value,
	rv.decimal_value,
	rv.boolean_value
FROM warehouse_central.dbo.redcap_data rv
JOIN warehouse_central.dbo.meta__redcap_instance mri 
	ON mri.id = rv.meta__redcap_instance_id
JOIN warehouse_central.dbo.meta__redcap_project mrp 
	ON mrp.id = rv.meta__redcap_project_id
JOIN warehouse_central.dbo.meta__redcap_arm mra 
	ON mra.id = rv.meta__redcap_arm_id 
JOIN warehouse_central.dbo.meta__redcap_event mre 
	ON mre.id = rv.meta__redcap_event_id
JOIN warehouse_central.dbo.meta__redcap_form mrf 
	ON mrf.id = rv.meta__redcap_form_id
JOIN warehouse_central.dbo.meta__redcap_form_section mrfs 
	ON mrfs.id = rv.meta__redcap_form_section_id
JOIN warehouse_central.dbo.meta__redcap_field mrf2 
	ON mrf2.id = rv.meta__redcap_field_id 
JOIN warehouse_central.dbo.redcap_participant rp 
	ON rp.id = rv.redcap_participant_id
LEFT JOIN warehouse_central.dbo.meta__redcap_field_enum mrfe 
	ON mrfe.id = rv.meta__redcap_field_enum_id 
JOIN warehouse_central.dbo.meta__redcap_data_type rdt
	ON rdt.id = rv.meta__redcap_data_type_id 
LEFT JOIN redcap_file rf 
	ON rf.id = rv.redcap_file_id
;