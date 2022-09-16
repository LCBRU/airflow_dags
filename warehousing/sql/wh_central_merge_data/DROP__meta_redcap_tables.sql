IF OBJECT_ID(N'redcap_value') IS NOT NULL
    DROP TABLE redcap_value

IF OBJECT_ID(N'redcap_participant') IS NOT NULL
    DROP TABLE redcap_participant

IF OBJECT_ID(N'meta__redcap_event') IS NOT NULL
    DROP TABLE meta__redcap_event

IF OBJECT_ID(N'meta__redcap_arm') IS NOT NULL
    DROP TABLE meta__redcap_arm

IF OBJECT_ID(N'meta__redcap_field_enum') IS NOT NULL
    DROP TABLE meta__redcap_field_enum

IF OBJECT_ID(N'meta__redcap_field') IS NOT NULL
    DROP TABLE meta__redcap_field

IF OBJECT_ID(N'meta__redcap_form_section') IS NOT NULL
    DROP TABLE meta__redcap_form_section

IF OBJECT_ID(N'meta__redcap_form') IS NOT NULL
    DROP TABLE meta__redcap_form

IF OBJECT_ID(N'meta__redcap_project') IS NOT NULL
    DROP TABLE meta__redcap_project

IF OBJECT_ID(N'meta__redcap_instance') IS NOT NULL
    DROP TABLE meta__redcap_instance
