IF OBJECT_ID(N'cfg_redcap_mapping') IS NULL
BEGIN
	CREATE TABLE cfg_redcap_mapping (
		id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
		cfg_study_id INT,
		cfg_redcap_instance_id INT,
		redcap_project_id INT,
		INDEX idx__cfg_redcap_mapping__cfg_study_id (cfg_study_id),
		INDEX idx__cfg_redcap_mapping__redcap_instance_id (cfg_redcap_instance_id),
		INDEX idx__cfg_redcap_mapping__redcap_project_id (redcap_project_id),
		FOREIGN KEY (cfg_redcap_instance_id) REFERENCES cfg_redcap_instance(id),
		FOREIGN KEY (cfg_study_id) REFERENCES cfg_study(id),
	);

	INSERT INTO cfg_redcap_mapping (cfg_study_id, redcap_project_id, cfg_redcap_instance_id) VALUES
	(1,93,1),
	(2,9,1),
	(3,68,1),
	(3,69,1),
	(4,39,1),
	(5,12,5),
	(5,13,5),
	(5,14,7),
	(5,28,1),
	(5,31,1),
	(5,77,1),
	(5,120,1),
	(6,64,1),
	(6,67,1),
	(7,12,2),
	(7,13,2),
	(7,14,2),
	(7,15,2),
	(7,16,2),
	(7,17,2),
	(7,18,2),
	(7,19,2),
	(7,24,1),
	(7,25,2),
	(7,26,2),
	(7,27,2),
	(8,56,1),
	(9,50,1),
	(9,54,1),
	(9,83,1),
	(10,26,1),
	(10,28,2),
	(10,29,1),
	(10,37,2),
	(10,54,2),
	(10,56,2),
	(10,59,2),
	(10,60,2),
	(12,43,1),
	(12,48,1),
	(13,32,4),
	(13,34,4),
	(13,36,4),
	(14,62,1),
	(14,63,1),
	(14,76,1),
	(15,84,1),
	(15,85,1),
	(15,89,1),
	(17,94,1),
	(17,96,1),
	(18,45,4),
	(19,91,1),
	(19,92,1),
	(20,5,1),
	(22,37,1),
	(23,40,5),
	(24,81,1),
	(25,43,4),
	(27,40,4),
	(28,13,7),
	(30,22,5),
	(31,108,1),
	(33,15,4),
	(33,51,1),
	(34,23,1),
	(35,16,5),
	(35,19,5),
	(35,29,5),
	(35,36,5),
	(35,42,5),
	(35,53,5),
	(37,8,1),
	(37,20,2),
	(37,21,2),
	(37,22,1),
	(37,22,2),
	(37,24,2),
	(39,13,6),
	(39,15,6),
	(39,22,6),
	(39,26,6),
	(39,28,6),
	(39,32,6),
	(39,33,6),
	(39,35,6),
	(39,38,6),
	(39,42,6),
	(40,66,2),
	(41,30,1),
	(42,12,4),
	(43,17,5),
	(43,18,5),
	(43,21,5),
	(44,17,4),
	(45,72,1),
	(46,53,2),
	(46,58,1),
	(46,66,1),
	(47,20,1),
	(48,37,5),
	(49,23,5),
	(50,60,1),
	(51,104,1),
	(51,105,1),
	(52,55,1),
	(53,16,4),
	(53,30,2),
	(53,31,2),
	(53,32,2),
	(53,33,2),
	(53,34,2),
	(53,35,2),
	(53,36,2),
	(53,52,1),
	(53,55,2),
	(53,57,2),
	(53,58,2),
	(55,25,5),
	(56,38,2),
	(56,39,2),
	(56,40,2),
	(56,41,2),
	(56,42,2),
	(56,43,2),
	(56,44,2),
	(56,45,2),
	(56,46,2),
	(56,47,2),
	(56,52,2),
	(56,61,2),
	(56,62,2),
	(56,64,2),
	(56,69,2),
	(57,14,4),
	(59,38,5),
	(60,87,1),
	(60,90,1),
	(61,70,2),
	(64,79,1),
	(66,30,5),
	(66,44,4),
	(67,111,1),
	(68,95,1),
	(69,101,1),
	(69,102,1),
	(70,25,1),
	(72,24,4),
	(73,19,4),
	(74,109,1),
	(75,29,4),
	(76,115,1),
	(80,20,5),
	(81,26,5),
	(82,27,5),
	(83,41,5),
	(83,52,5),
	(84,48,5),
	(85,49,5),
	(86,51,5),
	(87,73,2),
	(88,76,2),
	(89,16,7),
	(89,77,2),
	(90,15,7),
	(90,78,2),
	(91,17,7),
	(92,18,7),
	(93,4,1),
	(93,40,1),
	(94,49,1),
	(95,112,1),
	(96,122,1),
	(97,124,1),
	(98,53,4);
END