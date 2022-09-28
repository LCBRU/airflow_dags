CREATE UNIQUE INDEX uidx__redcap_value__unique_observations
ON redcap_value (
    meta__redcap_instance_id,
    meta__redcap_project_id,
    meta__redcap_arm_id,
    meta__redcap_event_id,
    meta__redcap_form_id,
    meta__redcap_form_section_id,
    meta__redcap_field_id,
    redcap_participant_id,
    meta__redcap_field_enum_id,
    instance
);
