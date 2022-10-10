SET QUOTED_IDENTIFIER OFF;
	
CREATE TABLE dbo.redcap_participant (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    meta__redcap_project_id INT NOT NULL,
    record NVARCHAR(500) NOT NULL,
    INDEX idx__meta__redcap_participant__record (record),
    FOREIGN KEY (meta__redcap_project_id) REFERENCES meta__redcap_project(id),
    UNIQUE (meta__redcap_project_id, record)
);

INSERT INTO warehouse_central.dbo.redcap_participant (meta__redcap_project_id, record)
SELECT DISTINCT
    mrp.id,
    rd.record
FROM warehouse_central.dbo.merged__redcap_data rd
JOIN warehouse_central.dbo.meta__redcap_instance mri
    ON mri.datalake_database = rd.datalake_database 
JOIN warehouse_central.dbo.meta__redcap_project mrp 
    ON mrp.meta__redcap_instance_id = mri.id 
    AND mrp.redcap_project_id = rd.project_id 

SET QUOTED_IDENTIFIER ON;
