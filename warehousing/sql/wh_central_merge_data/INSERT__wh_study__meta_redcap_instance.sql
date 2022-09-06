DECLARE @study_name NVARCHAR(500) 
DECLARE @sql NVARCHAR(MAX) 

DECLARE db_cursor CURSOR FOR 
SELECT DISTINCT study_name
FROM datalake_redcap_project_mappings

OPEN db_cursor  
FETCH NEXT FROM db_cursor INTO @study_name  

WHILE @@FETCH_STATUS = 0  
BEGIN
	SET @sql = N'
		INSERT INTO [wh_study_' + @study_name + '].dbo.meta__redcap_instance
		SELECT DISTINCT datalake_database
		FROM warehouse_central.dbo.datalake_redcap_project_mappings
		WHERE study_name = ''' + @study_name + ''';'
	EXECUTE sp_executesql @sql
	
	FETCH NEXT FROM db_cursor INTO @study_name
END 

CLOSE db_cursor
DEALLOCATE db_cursor 
