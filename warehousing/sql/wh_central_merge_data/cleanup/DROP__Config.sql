IF OBJECT_ID(N'cfg_openspecimen_study_mapping') IS NOT NULL
    DROP TABLE cfg_openspecimen_study_mapping;

IF OBJECT_ID(N'cfg_wh_participant_identifier_table_columns', N'U') IS NOT NULL  
    DROP TABLE cfg_wh_participant_identifier_table_columns;

IF OBJECT_ID(N'cfg_wh_participant_identifier_type', N'U') IS NOT NULL  
    DROP TABLE cfg_wh_participant_identifier_type;

IF OBJECT_ID(N'cfg_wh_participant_source', N'U') IS NOT NULL  
    DROP TABLE cfg_wh_participant_source;

IF OBJECT_ID(N'cfg_wh_redcap_instance', N'U') IS NOT NULL  
    DROP TABLE cfg_wh_redcap_instance;
