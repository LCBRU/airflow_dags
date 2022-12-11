SET QUOTED_IDENTIFIER OFF;
	
CREATE TABLE meta__redcap_form (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    meta__redcap_project_id INT NOT NULL,
    name NVARCHAR(500) NOT NULL,
    INDEX idx__redcap_form__name (name),
    FOREIGN KEY (meta__redcap_project_id) REFERENCES meta__redcap_project(id),
    UNIQUE (meta__redcap_project_id, name)
);

EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'datalake_redcap_%'
BEGIN 
	INSERT INTO warehouse_central.dbo.meta__redcap_form (meta__redcap_project_id, name)
    SELECT
        rp.id,
        rf.form_name
    FROM (
        SELECT DISTINCT
            '?' datalake_database,
            project_id,
            form_name
        FROM [?].dbo.redcap_metadata
    ) rf
    JOIN warehouse_central.dbo.cfg_redcap_instance ri
        ON ri.datalake_database = rf.datalake_database
    JOIN warehouse_central.dbo.meta__redcap_project rp
        ON  rp.cfg_redcap_instance_id = ri.id
        AND rp.redcap_project_id = rf.project_id
END"

SET QUOTED_IDENTIFIER ON;
