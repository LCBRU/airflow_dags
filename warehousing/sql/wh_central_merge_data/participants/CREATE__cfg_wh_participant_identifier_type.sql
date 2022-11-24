CREATE TABLE cfg_wh_participant_identifier_type (
	id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
	name nvarchar(500),
	INDEX idx__cfg_wh_participant_identifier_type__name (name),
);

-- NHS IDs
INSERT INTO cfg_wh_participant_identifier_type (name) VALUES
	(N'UHL_NUMBER'),
	(N'uhl_system_number'),
	(N'nhs_number');

-- Study Participant IDs
INSERT INTO cfg_wh_participant_identifier_type (name) VALUES
	(N'alleviate_id'),
	(N'amaze_id'),
	(N'AS_PROGRESSION_ID'),
	(N'BIORESOURCE_ID'),
	(N'BIORESOURCE_LEGACY_ID'),
	(N'BME_COVID_ID'),
	(N'BRAVE_ID'),
	(N'BREATHE_DEEP_ID'),
	(N'BRICCS_ID'),
	(N'CARDIOMET_ID'),
	(N'CARMER_BREATH_ID'),
	(N'COHERE_ID');
INSERT INTO cfg_wh_participant_identifier_type (name) VALUES
	(N'COPD_COVID_19_ID'),
	(N'CTO_ID'),
	(N'CVLPRIT_ID'),
	(N'CVLPRIT_LOCAL_ID'),
	(N'DESMOND_ID'),
	(N'DISCORDANCE_ID'),
	(N'DREAM_ID'),
	(N'EDEN_ID'),
	(N'EDIFY_ID'),
	(N'ELASTIC_AS_ID');
INSERT INTO cfg_wh_participant_identifier_type (name) VALUES
	(N'EMMACE4_ID'),
	(N'EPIGENE1_ID'),
	(N'EXTEND_ID'),
	(N'FAST_ID'),
	(N'FOAMI_ID'),
	(N'GENVASC_ID'),
	(N'GLOBAL_VIEWS_ID'),
	(N'GO_DCM_ID'),
	(N'GRAPHIC_LAB_ID'),
	(N'GRAPHIC2_ID'),
	(N'HAD_ID'),
	(N'INDAPAMIDE_ID');
INSERT INTO cfg_wh_participant_identifier_type (name) VALUES
	(N'INTERFIELD_ID'),
	(N'INTERVAL_ID'),
	(N'LENTEN_ID'),
	(N'LIMB_ID'),
	(N'MARI_ID'),
	(N'MEIRU_ID'),
	(N'MEL_ID'),
	(N'MI_ECMO_ID'),
	(N'MINERVA_ID'),
	(N'MULTI_MORBID_PRIORITIES_ID');
INSERT INTO cfg_wh_participant_identifier_type (name) VALUES
	(N'NON_ADHERENCE_ID'),
	(N'National_Bioresource_id'),
	(N'omics_id'),
	(N'PILOT_ID'),
	(N'PREDICT_ID'),
	(N'PREECLAMPSIA_ID'),
	(N'RAPID_NSTEMI_ID'),
	(N'REST_ID'),
	(N'SALT_ID'),
	(N'SCAD_CAE_ID'),
	(N'SCAD_ID'),
	(N'SCAD_LOCAL_ID');
INSERT INTO cfg_wh_participant_identifier_type (name) VALUES
	(N'SCAD_REG_ID'),
	(N'SCAD_SURVEY_ID'),
	(N'SKOPE_ID'),
	(N'SPACE_FOR_COPD_ID'),
	(N'SPIRAL_ID'),
	(N'tmao_id'),
	(N'UPFOR5_ID'),
	(N'VASCEGENS_ID');
INSERT INTO cfg_wh_participant_identifier_type (name) VALUES
	(N'yakult_id'),
	(N'yoga_id');

-- CiviCRM IDs
INSERT INTO cfg_wh_participant_identifier_type (name) VALUES
	(N'CiviCRM Case ID'),
	(N'CiviCRM Contact ID');

