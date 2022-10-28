SET QUOTED_IDENTIFIER OFF;
	
CREATE TABLE dbo.meta__redcap_project (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    meta__redcap_instance_id INT NOT NULL,
    redcap_project_id INT NOT NULL,
    name NVARCHAR(500) NOT NULL,
    INDEX idx__meta__redcap_project__name (name),
    FOREIGN KEY (meta__redcap_instance_id) REFERENCES meta__redcap_instance(id),
    UNIQUE (meta__redcap_instance_id, redcap_project_id),
    UNIQUE (meta__redcap_instance_id, name)
);

EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'datalake_redcap_%'
BEGIN 
	INSERT INTO warehouse_central.dbo.meta__redcap_project (meta__redcap_instance_id, redcap_project_id, name)
    SELECT
        ri.id,
        rp.project_id,
        rp.project_name
    FROM (
        SELECT
            '?' datalake_database,
            project_id,
            project_name
        FROM [?].dbo.redcap_projects
        WHERE project_id IN (
            SELECT DISTINCT redcap_project_id
            FROM warehouse_central.dbo.etl__redcap_project_mapping
            WHERE datalake_database = '?'
        )
    ) rp
    JOIN warehouse_central.dbo.meta__redcap_instance ri
        ON ri.datalake_database = rp.datalake_database
END"

SET QUOTED_IDENTIFIER ON;
