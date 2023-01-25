IF OBJECT_ID(N'cfg_openspecimen_study_mapping') IS NULL
BEGIN
    CREATE TABLE cfg_openspecimen_study_mapping (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        collection_protocol_id INT NOT NULL,
        cfg_study_id INT NOT NULL,
        cfg_participant_identifier_type_id INT NOT NULL,
        INDEX idx__cfg_openspecimen_study_mapping__collection_protocol_id (collection_protocol_id),
        INDEX idx__cfg_openspecimen_study_mapping__cfg_study_id (cfg_study_id),
        INDEX idx__cfg_openspecimen_study_mapping__cfg_participant_identifier_type_id (cfg_participant_identifier_type_id),
		FOREIGN KEY (cfg_study_id) REFERENCES cfg_study(id),
		FOREIGN KEY (cfg_participant_identifier_type_id) REFERENCES cfg_participant_identifier_type(id),
    )

    INSERT INTO cfg_openspecimen_study_mapping (collection_protocol_id, cfg_study_id, cfg_participant_identifier_type_id)
    SELECT
        a.collection_protocol_id,
        cs.id AS cfg_study_id,
        pit.id AS cfg_participant_identifier_type_id
    FROM (
        SELECT 1 as collection_protocol_id, 'BRICCS' as study_name, 'BRICCS_ID' as id_name
        UNION SELECT 3, 'Biostat', 'BIOSTAT_ID'
        UNION SELECT 6, 'prestige', 'PRESTIGE_ID'
        UNION SELECT 7, 'GENVASC', 'GENVASC_ID'
        UNION SELECT 8, 'DREAM', 'DREAM_ID'
        UNION SELECT 9, 'MVO', 'MVO_ID'
        UNION SELECT 10, 'BRICCS', 'BRICCS_ID'
        UNION SELECT 11, 'primid', 'PRIMID_ID'
        UNION SELECT 12, 'DHF', 'DHF_ID'
        UNION SELECT 14, 'GRAPHIC2', 'GRAPHIC2_ID'
        UNION SELECT 15, 'ultimate', 'ultimate_id'
        UNION SELECT 16, 'BRICCS', 'BRICCS_ID'
        UNION SELECT 17, 'BRAVE', 'BRAVE_ID'
        UNION SELECT 18, 'diastolic', 'DIASTOLIC_ID'
        UNION SELECT 19, 'SCAD', 'SCAD_ID'
        UNION SELECT 31, 'BRAVE', 'BRAVE_ID'
        UNION SELECT 38, 'BRAVE', 'BRAVE_ID'
        UNION SELECT 40, 'Pre-Eclampsia', 'PREECLAMPSIA_ID'
        UNION SELECT 41, 'Indapamide', 'INDAPAMIDE_ID'
        UNION SELECT 42, 'CAE', 'SCAD_CAE_ID'
        UNION SELECT 43, 'VasCeGenS', 'VASCEGENS_ID'
        UNION SELECT 44, 'LENTEN', 'LENTEN_ID'
        UNION SELECT 45, 'PREDICT', 'PREDICT_ID'
        UNION SELECT 46, 'Cardiomet', 'CARDIOMET_ID'
        UNION SELECT 47, 'ember', 'EMBER_ID'
        UNION SELECT 48, 'copd-st2op', 'copd_st2op_id'
        UNION SELECT 49, 'copd-had', 'copd_had_id'
        UNION SELECT 50, 'cascade', 'cascade_id'
        UNION SELECT 51, 'valcard', 'valcard_id'
        UNION SELECT 52, 'ild', 'ILD_ID'
        UNION SELECT 54, 'ponente', 'ponente_id'
        UNION SELECT 55, 'CIA', 'CIA_ID'
        UNION SELECT 56, 'DISCORDANCE', 'DISCORDANCE_ID'
        UNION SELECT 57, 'thermoplasty basal slides', 'thermoplasty_basal_id'
        UNION SELECT 58, 'ukags', 'ukags_id'
        UNION SELECT 59, 'pre-op energy', 'pre_op_energy_id'
        UNION SELECT 61, 'LIMb', 'LIMB_ID'
        UNION SELECT 62, 'REST', 'REST_ID'
        UNION SELECT 64, 'ELASTIC-AS', 'ELASTIC_AS_ID'
        UNION SELECT 66, 'ORFAN', 'ORFAN_ID'
        UNION SELECT 67, 'GO-DCM', 'GO_DCM_ID'
        UNION SELECT 68, 'gossamer', 'gossamer_id'
        UNION SELECT 69, 'ALLEVIATE', 'alleviate_id'
        UNION SELECT 70, 'chinook', 'chinook_id'
        UNION SELECT 71, 'copd-help', 'copd_help_id'
        UNION SELECT 72, 'course', 'course_id'
        UNION SELECT 73, 'ironman', 'ironman_id'
        UNION SELECT 74, 'ddtbd', 'ddtbd_id'
        UNION SELECT 77, 'bpf-gild', 'bpf_gild_id'
        UNION SELECT 78, 'p-vect', 'p_vect_id'
        UNION SELECT 79, 'CMR vs CT-FFR', 'CMR_VS_CT_ID'
        UNION SELECT 80, '4d Pharma', '4d_pharma_id'
        UNION SELECT 81, 'genentech', 'genentech_id'
        UNION SELECT 82, 'rasp', 'rasp_id'
        UNION SELECT 83, 'Beat-sa', 'beat_sa_id'
        UNION SELECT 84, 'Beat-sa', 'beat_sa_id'
        UNION SELECT 85, 'Benrex', 'Benrex_id'
        UNION SELECT 86, 'covid-heart', 'covid_heart_id'
        UNION SELECT 87, 'mri uspio', 'mri_uspio_id'
        UNION SELECT 88, 'event', 'event_id'
        UNION SELECT 89, 'CHESTY', 'CHESTY_ID'
        UNION SELECT 90, 'mesos lcm', 'mesos_lcm_id'
        UNION SELECT 91, 'COSMIC', 'COSMIC_ID'
        UNION SELECT 92, 'AZ Mahale', 'az_mahale_id'
        UNION SELECT 93, 'ironman', 'ironman_id'
        UNION SELECT 95, 'CARLOTA', 'CARLOTA_ID'
    ) a
    JOIN cfg_participant_identifier_type pit
        ON pit.name = a.id_name
    JOIN cfg_study cs
        ON cs.name = a.study_name
END