IF OBJECT_ID(N'cfg_openspecimen_study_mapping') IS NULL
BEGIN
    CREATE TABLE cfg_openspecimen_study_mapping (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        collection_protocol_id INT NOT NULL,
        study_id INT NOT NULL,
        participant_identifier_type_id INT NOT NULL,
        INDEX idx__cfg_openspecimen_study_mapping__collection_protocol_id (collection_protocol_id),
        INDEX idx__cfg_openspecimen_study_mapping__study_id (study_id),
        INDEX idx__cfg_openspecimen_study_mapping__participant_identifier_type_id (participant_identifier_type_id),
    )

    INSERT INTO warehouse_central.dbo.cfg_openspecimen_study_mapping (collection_protocol_id, study_id, participant_identifier_type_id) VALUES
        (1,7, (SELECT id FROM cfg_participant_identifier_type WHERE name='BRICCS_ID')),
        (7,46, (SELECT id FROM cfg_participant_identifier_type WHERE name='GENVASC_ID')),
        (8,37, (SELECT id FROM cfg_participant_identifier_type WHERE name='DREAM_ID')),
        (10,7, (SELECT id FROM cfg_participant_identifier_type WHERE name='BRICCS_ID')),
        (12,36, (SELECT id FROM cfg_participant_identifier_type WHERE name='DHF_ID')),
        (14,47, (SELECT id FROM cfg_participant_identifier_type WHERE name='GRAPHIC2_ID')),
        (16,7, (SELECT id FROM cfg_participant_identifier_type WHERE name='BRICCS_ID')),
        (17,10, (SELECT id FROM cfg_participant_identifier_type WHERE name='BRAVE_ID')),
        (19,5, (SELECT id FROM cfg_participant_identifier_type WHERE name='SCAD_ID')),
        (31,10, (SELECT id FROM cfg_participant_identifier_type WHERE name='BRAVE_ID')),
        (38,10, (SELECT id FROM cfg_participant_identifier_type WHERE name='BRAVE_ID')),
        (40,4, (SELECT id FROM cfg_participant_identifier_type WHERE name='PREECLAMPSIA_ID')),
        (41,9, (SELECT id FROM cfg_participant_identifier_type WHERE name='INDAPAMIDE_ID')),
        (42,1, (SELECT id FROM cfg_participant_identifier_type WHERE name='SCAD_CAE_ID')),
        (43,73, (SELECT id FROM cfg_participant_identifier_type WHERE name='VASCEGENS_ID')),
        (44,8, (SELECT id FROM cfg_participant_identifier_type WHERE name='LENTEN_ID')),
        (45,14, (SELECT id FROM cfg_participant_identifier_type WHERE name='PREDICT_ID')),
        (46,6, (SELECT id FROM cfg_participant_identifier_type WHERE name='CARDIOMET_ID')),
        (55,16, (SELECT id FROM cfg_participant_identifier_type WHERE name='CIA_ID')),
        (56,15, (SELECT id FROM cfg_participant_identifier_type WHERE name='DISCORDANCE_ID')),
        (61,13, (SELECT id FROM cfg_participant_identifier_type WHERE name='LIMB_ID')),
        (62,66, (SELECT id FROM cfg_participant_identifier_type WHERE name='REST_ID')),
        (64,17, (SELECT id FROM cfg_participant_identifier_type WHERE name='ELASTIC_AS_ID')),
        (67,19, (SELECT id FROM cfg_participant_identifier_type WHERE name='GO_DCM_ID')),
        (69,18, (SELECT id FROM cfg_participant_identifier_type WHERE name='alleviate_id')),
        (79,76, (SELECT id FROM cfg_participant_identifier_type WHERE name='CMR_VS_CT_ID')),
        (89,97, (SELECT id FROM cfg_participant_identifier_type WHERE name='CHESTY_ID')),
        (91,79, (SELECT id FROM cfg_participant_identifier_type WHERE name='COSMIC_ID')),
        (95,96, (SELECT id FROM cfg_participant_identifier_type WHERE name='CARLOTA_ID'));
END