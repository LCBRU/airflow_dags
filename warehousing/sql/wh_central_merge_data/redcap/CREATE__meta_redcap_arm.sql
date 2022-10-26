SET QUOTED_IDENTIFIER OFF;
	
CREATE TABLE dbo.meta__redcap_arm (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    meta__redcap_project_id INT NOT NULL,
    redcap_arm_id INT NOT NULL,
    arm_num INT NOT NULL,
    name NVARCHAR(500) NOT NULL,
    INDEX idx__meta__redcap_arm__name (name),
    FOREIGN KEY (meta__redcap_project_id) REFERENCES meta__redcap_project(id),
    UNIQUE (meta__redcap_project_id, redcap_arm_id),
    UNIQUE (meta__redcap_project_id, name)
);

EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'datalake_redcap_%'
BEGIN 
	INSERT INTO warehouse_central.dbo.meta__redcap_arm (meta__redcap_project_id, redcap_arm_id, arm_num, name)
    SELECT DISTINCT
        mrp.id AS meta__redcap_project_id,
        rea.arm_id,
        rea.arm_num,
        rea.arm_name 
    FROM [?].dbo.redcap_events_arms rea 
    JOIN warehouse_central.dbo.meta__redcap_instance mri
        ON mri.datalake_database = '?'
    JOIN warehouse_central.dbo.meta__redcap_project mrp 
        ON mrp.meta__redcap_instance_id = mri.id 
        AND mrp.redcap_project_id = rea.project_id 
END"

SET QUOTED_IDENTIFIER ON;
