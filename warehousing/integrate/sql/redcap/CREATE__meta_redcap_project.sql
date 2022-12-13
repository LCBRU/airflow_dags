SET QUOTED_IDENTIFIER OFF;
	
CREATE TABLE dbo.meta__redcap_project (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    cfg_redcap_instance_id INT NOT NULL,
    redcap_project_id INT NOT NULL,
    name NVARCHAR(500) NOT NULL,
    INDEX idx__meta__redcap_project__name (name),
    UNIQUE (cfg_redcap_instance_id, redcap_project_id),
    UNIQUE (cfg_redcap_instance_id, name)
);

EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'datalake_redcap_%'
BEGIN 
	INSERT INTO warehouse_central.dbo.meta__redcap_project (cfg_redcap_instance_id, redcap_project_id, name)
    SELECT
        ri.cfg_redcap_instance_id,
        rp.project_id,
        rp.project_name
    FROM [?].dbo.redcap_projects rp
    JOIN (
        SELECT DISTINCT
            redcap_project_id,
            ri.id AS cfg_redcap_instance_id
        FROM warehouse_config.dbo.cfg_redcap_instance ri
        JOIN warehouse_config.dbo.cfg_redcap_mapping rm
            ON rm.cfg_redcap_instance_id = ri.id
        WHERE ri.datalake_database = '?'
    ) ri ON ri.redcap_project_id = rp.project_id
END"

SET QUOTED_IDENTIFIER ON;
