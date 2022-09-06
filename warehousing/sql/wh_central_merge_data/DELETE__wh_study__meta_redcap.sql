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
		DELETE FROM [wh_study_' + @study_name + '].dbo.meta__redcap_field_enum;
		DELETE FROM [wh_study_' + @study_name + '].dbo.meta__redcap_field;
		DELETE FROM [wh_study_' + @study_name + '].dbo.meta__redcap_form_section;
		DELETE FROM [wh_study_' + @study_name + '].dbo.meta__redcap_form;
		DELETE FROM [wh_study_' + @study_name + '].dbo.meta__redcap_project;
		DELETE FROM [wh_study_' + @study_name + '].dbo.meta__redcap_instance;'
	EXECUTE sp_executesql @sql
	
	FETCH NEXT FROM db_cursor INTO @study_name
END 

CLOSE db_cursor
DEALLOCATE db_cursor 
