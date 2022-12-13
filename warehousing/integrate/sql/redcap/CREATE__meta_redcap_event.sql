SET QUOTED_IDENTIFIER OFF;
	
CREATE TABLE dbo.meta__redcap_event (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    meta__redcap_arm_id INT NOT NULL,
    redcap_event_id INT NOT NULL,
    day_offset INT NOT NULL,
    offset_min INT NOT NULL,
    offset_max INT NOT NULL,
    name NVARCHAR(500) NOT NULL,
    INDEX idx__meta__redcap_event__name (name),
    FOREIGN KEY (meta__redcap_arm_id) REFERENCES meta__redcap_arm(id),
    UNIQUE (meta__redcap_arm_id, redcap_event_id),
    UNIQUE (meta__redcap_arm_id, name),
);

EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'datalake_redcap_%'
BEGIN 
	INSERT INTO warehouse_central.dbo.meta__redcap_event (meta__redcap_arm_id, redcap_event_id, day_offset, offset_min, offset_max, name)
    SELECT DISTINCT
        mra.id AS meta__redcap_arm_id,
        rem.event_id AS redcap_event_id,
        rem.day_offset,
        rem.offset_min,
        rem.offset_max,
        rem.descrip
    FROM [?].dbo.redcap_events_metadata rem
    JOIN warehouse_config.dbo.cfg_redcap_instance mri
        ON mri.datalake_database = '?'
    JOIN warehouse_central.dbo.meta__redcap_project mrp 
        ON mrp.cfg_redcap_instance_id = mri.id 
    JOIN warehouse_central.dbo.meta__redcap_arm	mra
        ON mra.meta__redcap_project_id = mrp.id
        AND mra.redcap_arm_id = rem.arm_id 
END"

SET QUOTED_IDENTIFIER ON;
