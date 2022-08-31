IF OBJECT_ID(N'dbo.redcap_instances', N'U') IS NOT NULL  
    DROP TABLE warehouse_central.dbo.redcap_instances;

CREATE TABLE warehouse_central.dbo.redcap_instances (
    datalake_db VARCHAR(255),
    redcap_version VARCHAR(255),
    redcap_base_url VARCHAR(2000)
)

EXEC sp_MSforeachdb
@command1='IF ''?'' LIKE ''datalake_redcap_%''
BEGIN 
	INSERT INTO warehouse_central.dbo.redcap_instances (datalake_db, redcap_version, redcap_base_url)
	SELECT ''?'' datalake_db, *  
	FROM  
	(
        SELECT *   
        FROM ?.dbo.redcap_config rc 
	) AS SourceTable  
	PIVOT
	(  
        MIN(value)
        FOR field_name IN ([redcap_version], [redcap_base_url])  
	) AS PivotTable
END'
