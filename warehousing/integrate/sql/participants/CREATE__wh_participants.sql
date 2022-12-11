CREATE TABLE wh_participants (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    identifier VARCHAR(200) NOT NULL,
    identifier_type_id INT NOT NULL,
    source_type_id INT NOT NULL,
    source_identifier INT NOT NULL,
    merged_participant_id INT,
    INDEX idx__wh_participants__identifier (identifier, identifier_type_id),
    INDEX idx__wh_participants__source_identifier (source_identifier, source_type_id),
    UNIQUE(identifier, identifier_type_id, source_type_id, source_identifier),
    FOREIGN KEY (identifier_type_id) REFERENCES cfg_participant_identifier_type(id),
    FOREIGN KEY (source_type_id) REFERENCES cfg_participant_identifier_type(id),
);
