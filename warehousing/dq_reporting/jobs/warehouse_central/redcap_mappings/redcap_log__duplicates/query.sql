SELECT rl.*
FROM desc__redcap_log rl
JOIN (
	SELECT 
		log_event_id,
        meta__redcap_field_id,
        meta__redcap_event_id,
        redcap_participant_id,
        meta__redcap_field_enum_id,
        field_name,
        instance,
        action_datetime,
        action_type,
        action_description
	FROM redcap_log
	GROUP BY
		log_event_id,
        meta__redcap_field_id,
        meta__redcap_event_id,
        redcap_participant_id,
        meta__redcap_field_enum_id,
        field_name,
        instance,
        action_datetime,
        action_type,
        action_description
        HAVING COUNT(*) > 1
) dups
	ON dups.log_event_id = rl.log_event_id
	AND ISNULL(dups.meta__redcap_field_id, -1) = ISNULL(rl.meta__redcap_field_id, -1)
	AND dups.meta__redcap_event_id = rl.meta__redcap_event_id
	AND dups.redcap_participant_id = rl.redcap_participant_id
	AND ISNULL(dups.meta__redcap_field_enum_id, -1) = ISNULL(rl.meta__redcap_field_enum_id, -1)
	AND dups.field_name = rl.field_name
	AND dups.instance = rl.[instance]
	AND dups.action_datetime = rl.action_datetime
	AND dups.action_type = rl.action_type
	AND ISNULL(dups.action_description, '') = ISNULL(rl.action_description, '')
;