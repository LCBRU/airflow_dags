CREATE TABLE cfg_study (
	id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
	name nvarchar(500) NOT NULL,
	edge_id INT NULL,
	INDEX idx__cfg_study__source_database (name),
);

SET IDENTITY_INSERT cfg_study ON;

INSERT INTO cfg_study (id,name,edge_id) VALUES
    (1,N'CAE',50359),
    (2,N'Bioresource',NULL),
    (3,N'SPIRAL',NULL),
    (4,N'Pre-Eclampsia',NULL),
    (5,N'SCAD',129226),
    (6,N'Cardiomet',101864),
    (7,N'BRICCS',16059),
    (8,N'LENTEN',88905),
    (9,N'Indapamide',88136),
    (10,N'BRAVE',51674);
INSERT INTO cfg_study (id,name,edge_id) VALUES
    (11,N'MERMAID',NULL),
    (12,N'FAST',88249),
    (13,N'LIMb',112906),
    (14,N'PREDICT',97183),
    (15,N'DISCORDANCE',110998),
    (16,N'CIA',NULL),
    (17,N'ELASTIC-AS',112788),
    (18,N'ALLEVIATE',NULL),
    (19,N'GO-DCM',107797),
    (20,N'Pilot',NULL);
INSERT INTO cfg_study (id,name,edge_id) VALUES
    (21,N'HIC Covid 19',NULL),
    (22,N'AS Progression',NULL),
    (23,N'BME COVID',NULL),
    (24,N'BRICCS_CT',NULL),
    (25,N'Breathe Deep',NULL),
    (26,N'Breathlessness',NULL),
    (27,N'CARMER BREATH',NULL),
    (28,N'CHABLIS',129780),
    (29,N'CMR Guide',NULL),
    (30,N'COHERE',105410);
INSERT INTO cfg_study (id,name,edge_id) VALUES
    (31,N'COPD_COVID_19',132765),
    (32,N'COPD INTROL',NULL),
    (33,N'CTO',NULL),
    (34,N'CVLPRIT',6439),
    (35,N'DESMOND',52352),
    (36,N'DHF',NULL),
    (37,N'DREAM',20305),
    (38,N'Dal-Gene',NULL),
    (39,N'EASY AS',90865),
    (40,N'EDEN',NULL);
INSERT INTO cfg_study (id,name,edge_id) VALUES
    (41,N'EDIFY',NULL),
    (42,N'EPIGENE1',NULL),
    (43,N'EXTEND',NULL),
    (44,N'FOAMI',88510),
    (45,N'FORECAST BRICCSCT ORFAN SCREENING',NULL),
    (46,N'GENVASC',13350),
    (47,N'GRAPHIC2',NULL),
    (48,N'Global_Views',NULL),
    (49,N'HAD',NULL),
    (50,N'Heart Failure Screening',NULL);
INSERT INTO cfg_study (id,name,edge_id) VALUES
    (51,N'INTERFIELD',NULL),
    (52,N'INTERVAL',NULL),
    (53,N'MARI',88015),
    (54,N'MCCANN_IMAGING',NULL),
    (55,N'MEL',NULL),
    (56,N'MINERVA',51707),
    (57,N'MI-ECMO',47133),
    (58,N'MRP_HFPEF',NULL),
    (59,N'Multi Morbid Priorities',NULL),
    (60,N'NON_ADHERENCE',NULL);
INSERT INTO cfg_study (id,name,edge_id) VALUES
    (61,N'NOVO5K',NULL),
    (62,N'PARC',109501),
    (63,N'PHOSP_COVID19',134402),
    (64,N'RAPID_NSTEMI',89678),
    (65,N'RECHARGE',NULL),
    (66,N'REST',NULL),
    (67,N'SALT',122377),
    (68,N'SKOPE',NULL),
    (69,N'SPACE_FOR_COPD',NULL),
    (70,N'TMAO',NULL);
INSERT INTO cfg_study (id,name,edge_id) VALUES
    (71,N'UHL_HCW_COVID_19',NULL),
    (72,N'Upfor5',NULL),
    (73,N'VasCeGenS',NULL),
    (74,N'YAKULT',128800),
    (75,N'YOGA',107299),
    (76,N'CMR vs CT-FFR',92855),
    (77,N'GRAPHIC',34117),
    (78,N'OMICS_REGISTER',NULL),
    (79,N'COSMIC',NULL),
    (80,N'BRC Information Governance',NULL);
INSERT INTO cfg_study (id,name,edge_id) VALUES
    (81,N'NIHR PhDs',NULL),
    (82,N'LDC Volunteers',NULL),
    (83,N'BRC PPI',NULL),
    (84,N'Chronic Disease in Care Homes COVID 19',NULL),
    (85,N'SLIMCARD',NULL),
    (86,N'COVID-19 Vaccine Hesitancy',NULL),
    (87,N'PRC',NULL),
    (88,N'MMQ',NULL),
    (89,N'MRI in Asymptomatic AS',NULL),
    (90,N'SHOCC',NULL);
INSERT INTO cfg_study (id,name,edge_id) VALUES
    (91,N'CRISP',NULL),
    (92,N'TARGET',NULL),
    (93,N'BRC Cardiovascular',NULL),
    (94,N'BRC IT',NULL),
    (95,N'Foot and Ankle Surgery',NULL),
    (96,N'CARLOTA',NULL),
    (97,N'CHESTY',NULL),
    (98,N'RESET',NULL),
    (99,N'EMMACE4',NULL),
    (101,N'Simon',NULL);
INSERT INTO cfg_study (id,name,edge_id) VALUES
    (102,N'FORECAST',NULL),
    (103,N'ORFAN',NULL),
    (104,N'Imaging Registry',NULL),
    (105,N'Risk Perception Survey',NULL),
    (106,N'Obesity UK',NULL),
    (108,N'IP',NULL),
    (109,N'Rehabilitation for Cardiac Arrhythmia',NULL),
    (110,N'4d Pharma',NULL),
    (111,N'AZ Mahale',NULL),
    (112,N'Beat-sa',NULL);
INSERT INTO cfg_study (id,name,edge_id) VALUES
    (113,N'Benrex',NULL),
    (114,N'Biostat',NULL),
    (115,N'bpf-gild',NULL),
    (116,N'cascade',NULL),
    (117,N'chinook',NULL),
    (118,N'copd-had',NULL),
    (119,N'copd-help',NULL),
    (120,N'copd-st2op',NULL),
    (121,N'course',NULL),
    (122,N'covid-heart',NULL);
INSERT INTO cfg_study (id,name,edge_id) VALUES
    (123,N'ddtbd',NULL),
    (124,N'diastolic',NULL),
    (125,N'ember',NULL),
    (126,N'event',NULL),
    (127,N'genentech',NULL),
    (128,N'gossamer',NULL),
    (129,N'ild',NULL),
    (130,N'ironman',NULL),
    (131,N'liquid nitrogen storage',NULL),
    (132,N'mesenchymal bank',NULL);
INSERT INTO cfg_study (id,name,edge_id) VALUES
    (133,N'mesos lcm',NULL),
    (134,N'mri uspio',NULL),
    (135,N'mvo',NULL),
    (136,N'ponente',NULL),
    (137,N'pre-op energy',NULL),
    (138,N'prestige',NULL),
    (139,N'primid',NULL),
    (140,N'p-vect',NULL),
    (141,N'ranolazine',NULL),
    (142,N'rasp',NULL);
INSERT INTO cfg_study (id,name,edge_id) VALUES
    (143,N'valcard',NULL),
    (144,N'thermoplasty basal slides',NULL),
    (145,N'ticonc',NULL),
    (146,N'ukags',NULL),
    (147,N'ultimate',NULL),
    (148,N'GLOBAL_LEADERS',NULL),
    (149,N'AMAZE',NULL);

SET IDENTITY_INSERT cfg_study OFF;
