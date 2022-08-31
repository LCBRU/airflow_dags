EXEC sp_MSforeachdb
@command1='IF ''?'' LIKE ''wh_study_%''
BEGIN 
	DROP DATABASE [?]
END'

DECLARE @name NVARCHAR(500) 
DECLARE @sql NVARCHAR(MAX) 

DECLARE db_cursor CURSOR FOR 
SELECT DISTINCT study_name
FROM datalake_redcap_project_mappings

OPEN db_cursor  
FETCH NEXT FROM db_cursor INTO @name  

WHILE @@FETCH_STATUS = 0  
BEGIN
	SET @sql = N'CREATE DATABASE [wh_study_' + @name + ']'
	EXECUTE sp_executesql @sql
	
	SET @sql = N'ALTER DATABASE [wh_study_' + @name + '] SET RECOVERY SIMPLE'
	EXECUTE sp_executesql @sql
	
	FETCH NEXT FROM db_cursor INTO @name
END 

CLOSE db_cursor  
DEALLOCATE db_cursor 
