SET QUOTED_IDENTIFIER OFF;
	
CREATE TABLE dbo.redcap_participant (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    meta__redcap_project_id INT NOT NULL,
    record NVARCHAR(500) NOT NULL,
    INDEX idx__meta__redcap_participant__record (record),
    FOREIGN KEY (meta__redcap_project_id) REFERENCES meta__redcap_project(id),
    UNIQUE (meta__redcap_project_id, record)
);

SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX)
DECLARE @database_name VARCHAR(255)
DECLARE @cfg_redcap_instance_id INT

DECLARE TABLE_CURSOR CURSOR
    LOCAL STATIC READ_ONLY FORWARD_ONLY
FOR
	SELECT id, datalake_database
	FROM cfg_redcap_instance 

OPEN TABLE_CURSOR
FETCH NEXT FROM TABLE_CURSOR INTO @cfg_redcap_instance_id, @database_name
WHILE @@FETCH_STATUS = 0
BEGIN

    SELECT @SQL = '
INSERT INTO warehouse_central.dbo.redcap_participant (meta__redcap_project_id, record)
SELECT DISTINCT
    mrp.id,
    rd.record
FROM ' + @database_name + '.dbo.redcap_data rd
JOIN warehouse_central.dbo.meta__redcap_project mrp 
    ON mrp.cfg_redcap_instance_id = ' + CONVERT(NVARCHAR(10), @cfg_redcap_instance_id) + ' 
    AND mrp.redcap_project_id = rd.project_id
'

    EXEC sp_executesql @SQL

	FETCH NEXT FROM TABLE_CURSOR INTO @cfg_redcap_instance_id, @database_name
END
CLOSE TABLE_CURSOR
DEALLOCATE TABLE_CURSOR

SET QUOTED_IDENTIFIER ON;
