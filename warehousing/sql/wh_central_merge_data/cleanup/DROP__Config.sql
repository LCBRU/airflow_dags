IF OBJECT_ID(N'cfg_openspecimen_study_mapping') IS NOT NULL
    DROP TABLE cfg_openspecimen_study_mapping;

IF OBJECT_ID(N'cfg_participant_identifier_table_columns', N'U') IS NOT NULL  
    DROP TABLE cfg_participant_identifier_table_columns;

IF OBJECT_ID(N'cfg_participant_identifier_type', N'U') IS NOT NULL  
    DROP TABLE cfg_participant_identifier_type;

IF OBJECT_ID(N'cfg_participant_source', N'U') IS NOT NULL  
    DROP TABLE cfg_participant_source;

IF OBJECT_ID(N'cfg_redcap_mapping', N'U') IS NOT NULL  
    DROP TABLE cfg_redcap_mapping;

IF OBJECT_ID(N'cfg_redcap_instance', N'U') IS NOT NULL  
    DROP TABLE cfg_redcap_instance;

IF OBJECT_ID(N'cfg_study', N'U') IS NOT NULL  
    DROP TABLE cfg_study;
