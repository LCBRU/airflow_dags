SELECT rv.*
FROM desc__redcap_data rv
JOIN (
	SELECT 
        cfg_redcap_instance_id,
        meta__redcap_project_id,
        meta__redcap_arm_id,
        meta__redcap_event_id,
        meta__redcap_form_id,
        meta__redcap_form_section_id,
        meta__redcap_field_id,
        redcap_participant_id,
        meta__redcap_field_enum_id,
        instance
	FROM redcap_data
	GROUP BY
        cfg_redcap_instance_id,
        meta__redcap_project_id,
        meta__redcap_arm_id,
        meta__redcap_event_id,
        meta__redcap_form_id,
        meta__redcap_form_section_id,
        meta__redcap_field_id,
        redcap_participant_id,
        meta__redcap_field_enum_id,
        instance
	HAVING COUNT(*) > 1
) dups
	ON dups.cfg_redcap_instance_id = rv.cfg_redcap_instance_id
	AND dups.meta__redcap_project_id = rv.meta__redcap_project_id
	AND dups.meta__redcap_arm_id = rv.meta__redcap_arm_id
	AND dups.meta__redcap_event_id = rv.meta__redcap_event_id
	AND dups.meta__redcap_form_id = rv.meta__redcap_form_id
	AND dups.meta__redcap_form_section_id = rv.meta__redcap_form_section_id
	AND dups.meta__redcap_field_id = rv.meta__redcap_field_id
	AND dups.redcap_participant_id = rv.redcap_participant_id
	AND ISNULL(dups.meta__redcap_field_enum_id, -1) = ISNULL(rv.meta__redcap_field_enum_id, -1)
	AND dups.instance = rv.instance
;