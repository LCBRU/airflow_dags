IF OBJECT_ID(N'cfg_participant_identifier_table_column') IS NULL
BEGIN
    CREATE TABLE cfg_participant_identifier_table_column (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        cfg_participant_identifier_type_id INT NOT NULL,
        cfg_participant_source_id INT NOT NULL,
        table_name VARCHAR(200) NOT NULL,
        identifier_column_name VARCHAR(200) NOT NULL,
        source_identifier_column_name VARCHAR(200) NOT NULL,
        INDEX idx__cfg_participant_identifier_table_columns__cfg_participant_identifier_type_id (cfg_participant_identifier_type_id),
        INDEX idx__cfg_participant_identifier_table_columns__cfg_participant_source_id (cfg_participant_source_id),
        UNIQUE(table_name, cfg_participant_identifier_type_id, identifier_column_name),
        FOREIGN KEY (cfg_participant_identifier_type_id) REFERENCES cfg_participant_identifier_type(id),
        FOREIGN KEY (cfg_participant_source_id) REFERENCES cfg_participant_source(id),	
    );

    INSERT INTO cfg_participant_identifier_table_column (cfg_participant_identifier_type_id, cfg_participant_source_id, table_name, identifier_column_name, source_identifier_column_name) VALUES
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='CiviCRM Case ID'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm__case',
        'id',
        'id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='CiviCRM Contact ID'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm__case',
        'contact_id',
        'id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='CiviCRM Contact ID'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Contact'),
        'civicrm__contact',
        'id',
        'id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='nhs_number'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Contact'),
        'civicrm__contact',
        'nhs_number',
        'id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='uhl_system_number'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Contact'),
        'civicrm__contact',
        'uhl_system_number',
        'id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='amaze_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_amaze',
        'amaze_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='brave_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_brave',
        'brave_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='briccs_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_brave',
        'briccs_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='briccs_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_briccs_recruitment_data',
        'briccs_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='cardiomet_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_cardiomet',
        'cardiomet_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='discordance_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_discordance',
        'discordance_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='dream_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_dream_recruitment_data',
        'dream_study_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='emmace4_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_emmace_4_recruitment_data',
        'emmace_4_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='fast_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_fast',
        'fast_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='foami_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_foami',
        'foami_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='genvasc_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_genvasc_recruitment_data',
        'genvasc_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='graphic_lab_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_graphic2',
        'graphic_lab_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='graphic2_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_graphic2',
        'graphic_participant_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='indapamide_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_indapamide',
        'indapamide_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='interval_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_interval_data',
        'interval_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='lenten_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_lenten',
        'lenten_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='limb_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_limb',
        'limb_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='National_Bioresource_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_national_bioresource',
        'national_bioresource_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='Bioresource_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_national_bioresource',
        'leicester_bioresource_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='BIORESOURCE_LEGACY_ID'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_national_bioresource',
        'legacy_bioresource_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='BIORESOURCE_LEGACY_ID'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_nihr_bioresource',
        'nihr_bioresource_legacy_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='BIORESOURCE_ID'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_nihr_bioresource',
        'nihr_bioresource_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='omics_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_omics_register',
        'omics_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='predict_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_predict',
        'predict_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='preeclampsia_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_preeclampsia',
        'preeclampsia_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='scad_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_scad',
        'scad_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='briccs_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_scad',
        'briccs_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='SCAD_REG_ID'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_scad',
        'scad_registry_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='SCAD_REG_ID'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_scad_register',
        'scad_registry_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='SPIRAL_ID'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_spiral',
        'spiral_id',
        'case_id'
    ),
    (
        (SELECT id FROM cfg_participant_identifier_type WHERE name='tmao_id'),
        (SELECT id FROM cfg_participant_source WHERE name='CiviCRM Case'),
        'civicrm_value_tmao',
        'tmao_id',
        'case_id'
    )
    ;
END