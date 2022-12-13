CREATE TABLE wh_merged_participant (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    identifier VARCHAR(200) NOT NULL,
    cfg_participant_identifier_type_id INT NOT NULL,
    cfg_participant_source_id INT NOT NULL,
    source_identifier INT NOT NULL,
    merged_participant_id INT,
    INDEX idx__wh_merged_participant__identifier (identifier, cfg_participant_identifier_type_id),
    INDEX idx__wh_merged_participant__source_identifier (source_identifier, cfg_participant_source_id),
    UNIQUE(identifier, cfg_participant_identifier_type_id, cfg_participant_source_id, source_identifier),
);
