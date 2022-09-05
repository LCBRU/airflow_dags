IF OBJECT_ID(N'dbo.redcap_instances', N'U') IS NOT NULL  
    DROP TABLE warehouse_central.dbo.redcap_instances;

CREATE TABLE warehouse_central.dbo.redcap_instances (
    datalake_database VARCHAR(255),
    source_database VARCHAR(255),
    redcap_version VARCHAR(255),
    redcap_base_url VARCHAR(2000)
)

EXEC sp_MSforeachdb
@command1='IF ''?'' LIKE ''datalake_redcap_%''
BEGIN 
	INSERT INTO warehouse_central.dbo.redcap_instances (datalake_database, source_database, redcap_version, redcap_base_url)
	SELECT
        ''?'' datalake_db,
        CASE ''?''
            WHEN ''datalake_redcap_uhl'' THEN ''redcap6170_briccs''
            WHEN ''datalake_redcap_n3'' THEN ''redcap6170_briccsext''
            WHEN ''datalake_redcap_uol'' THEN ''uol_crf_redcap''
            WHEN ''datalake_redcap_internet'' THEN ''uol_survey_redcap''
            WHEN ''datalake_redcap_national'' THEN ''redcap_national''
            WHEN ''datalake_redcap_easyas'' THEN ''uol_easyas_redcap''
        END source_db,
        redcap_version,
        redcap_base_url
	FROM  
	(
        SELECT *   
        FROM [?].dbo.redcap_config rc 
	) AS SourceTable
	PIVOT
	(  
        MIN(value)
        FOR field_name IN ([redcap_version], [redcap_base_url])  
	) AS PivotTable
END'
