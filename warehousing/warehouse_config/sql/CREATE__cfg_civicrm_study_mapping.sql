IF OBJECT_ID(N'cfg_civicrm_study_mapping') IS NULL
BEGIN
    CREATE TABLE cfg_civicrm_study_mapping (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        case_type_id INT NOT NULL,
        cfg_study_id INT NOT NULL,
        INDEX idx__cfg_civicrm_study_mapping__collection_protocol_id (case_type_id),
        INDEX idx__cfg_civicrm_study_mapping__cfg_study_id (cfg_study_id),
		FOREIGN KEY (cfg_study_id) REFERENCES cfg_study(id),
    )

INSERT INTO cfg_civicrm_study_mapping (case_type_id,cfg_study_id) VALUES
    (3,46),
    (7,2),
    (21,2),
    (10,10),
    (6,7),
    (24,6),
    (29,15),
    (4,37),
    (8,99),
    (18,12),
    (20,44),
    (13,46),
    (11,48),
    (5,47),
    (19,9),
    (15,52),
    (22,8),
    (28,13),
    (17,11),
    (27,2),
    (14,78),
    (23,14),
    (26,4),
    (9,5),
    (30,5),
    (25,3),
    (12,70);
END