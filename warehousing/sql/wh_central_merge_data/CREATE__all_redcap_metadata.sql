	CREATE OR ALTER VIEW all_redcap_metadata AS
        SELECT 'datalake_redcap_easyas' datalake_database, rm.* 
        FROM datalake_redcap_easyas.dbo.redcap_metadata rm
        
        UNION ALL

        SELECT 'datalake_redcap_genvasc' datalake_database, rm.* 
        FROM datalake_redcap_genvasc.dbo.redcap_metadata rm	
        
        UNION ALL

        SELECT 'datalake_redcap_internet' datalake_database, rm.* 
        FROM datalake_redcap_internet.dbo.redcap_metadata rm	

        UNION ALL

        SELECT 'datalake_redcap_n3' datalake_database, rm.* 
        FROM datalake_redcap_n3.dbo.redcap_metadata rm	

        UNION ALL

        SELECT 'datalake_redcap_national' datalake_database, rm.* 
        FROM datalake_redcap_national.dbo.redcap_metadata rm

        UNION ALL

        SELECT 'datalake_redcap_uhl' datalake_database, rm.* 
        FROM datalake_redcap_uhl.dbo.redcap_metadata rm	

        UNION ALL

        SELECT 'datalake_redcap_uol' datalake_database, rm.* 
        FROM datalake_redcap_uol.dbo.redcap_metadata rm	