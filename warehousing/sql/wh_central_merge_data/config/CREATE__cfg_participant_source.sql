CREATE TABLE cfg_participant_source (
	id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
	name nvarchar(500),
	INDEX idx__cfg_participant_source__name (name),
);

-- NHS IDs
INSERT INTO cfg_participant_source (name) VALUES
	(N'REDCap'),
	(N'OpenSpecimen'),
	(N'CiviCRM Contact'),
	(N'CiviCRM Case');
