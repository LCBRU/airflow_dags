CREATE UNIQUE INDEX uidx__redcap_log__unique
ON redcap_log (
    meta__redcap_field_id,
    meta__redcap_event_id,
    redcap_participant_id,
    meta__redcap_field_enum_id,
    field_name,
    instance,
    action_datetime,
    action_type
);
