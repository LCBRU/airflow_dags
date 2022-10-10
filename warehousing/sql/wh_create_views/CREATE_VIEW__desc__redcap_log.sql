CREATE OR ALTER VIEW desc__redcap_log AS
SELECT
    rl.log_event_id,
	mri.datalake_database,
	mrp.name project_name,
	mrp.redcap_project_id,
	mra.name arm_name,
	mra.arm_num,
	rl.meta__redcap_event_id,
	mre.redcap_event_id,
	mre.name event_name,
	rl.redcap_participant_id,
	rp.record,
	rl.meta__redcap_field_id,
	rl.field_name,
	rl.[instance],
	drf.element_type,
	drf.element_validation_type,
	rl.username,
	rl.action_datetime,
	rl.action_type,
	rl.action_description,
	rl.data_value,
	rl.meta__redcap_field_enum_id,
	mrfe.name enum_name
FROM redcap_log rl
JOIN redcap_participant rp 
	ON rp.id = rl.redcap_participant_id
LEFT JOIN meta__redcap_field_enum mrfe
	ON mrfe.id = rl.meta__redcap_field_enum_id
LEFT JOIN desc__redcap_field drf 
	ON drf.meta__redcap_field_id = rl.meta__redcap_field_id 
JOIN meta__redcap_event mre 
	ON mre.id = rl.meta__redcap_event_id 
JOIN meta__redcap_arm mra 
	ON mra.id = mre.meta__redcap_arm_id 
JOIN meta__redcap_project mrp 
	ON mrp.id = mra.meta__redcap_project_id 
JOIN meta__redcap_instance mri 
	ON mri.id = mrp.meta__redcap_instance_id
;