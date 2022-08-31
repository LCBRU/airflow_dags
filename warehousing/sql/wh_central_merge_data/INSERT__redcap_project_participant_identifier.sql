		IF OBJECT_ID(N'dbo.redcap_project_participant_identifier', N'U') IS NOT NULL  
		    DROP TABLE warehouse_central.dbo.redcap_project_participant_identifier;
		
		CREATE TABLE redcap_project_participant_identifier (
            id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            database_name NVARCHAR(500) NOT NULL,
            redcap_project_id INT NOT NULL,
            record NVARCHAR(100) NOT NULL,
            cfg_participant_id_type_id INT NOT NULL,
            identifier NVARCHAR(100) NOT NULL,
            merged_participant_id INT NOT NULL
		);
		
		CREATE INDEX idx__redcap_project_participant_identifier__db_proj_record
		    ON redcap_project_participant_identifier(database_name, redcap_project_id, record)
		;
		
		CREATE INDEX idx__redcap_project_participant_identifier__cfg_participant_id_type_id_identifier
		    ON redcap_project_participant_identifier(cfg_participant_id_type_id, identifier)
		;

		CREATE INDEX idx__redcap_project_participant_identifier__merged_participant_id
		    ON redcap_project_participant_identifier(merged_participant_id)
		;

        CREATE TABLE #merging_participants (
            id INT IDENTITY(1,1) NOT NULL,
            database_name NVARCHAR(500) NOT NULL,
            redcap_project_id INT NOT NULL,
            record NVARCHAR(100) NOT NULL,
            identifier NVARCHAR(100) NOT NULL,
            type_id INT NOT NULL,
            merged_min_id INT NULL,
        );

        CREATE NONCLUSTERED INDEX #idx_merging_participants_id ON #merging_participants(id);
        CREATE NONCLUSTERED INDEX #idx_merging_participants_record ON #merging_participants(database_name, redcap_project_id, record);
        CREATE NONCLUSTERED INDEX #idx_merging_participants_identifier ON #merging_participants(identifier, type_id);

		EXEC sp_MSforeachdb
		@command1='IF ''?'' LIKE ''datalake_redcap_%''
		BEGIN
		    INSERT INTO #merging_participants(database_name, redcap_project_id, record, type_id, identifier)
		    SELECT
		        crif.database_name,
		        rd.project_id,
		        rd.record,
		        crif.cfg_participant_id_type_id,
		        RTRIM(LTRIM(rd.value))
		    FROM ?.dbo.redcap_data rd
		    JOIN warehouse_central.dbo.cfg_redcap_id_fields crif
		        ON crif.database_name = ''?''
		        AND crif.project_id = rd.project_id 
		        AND crif.field_name = rd.field_name
		    WHERE LEN(LTRIM(RTRIM(COALESCE(rd.value, '''')))) > 0
		END'

        UPDATE #merging_participants SET merged_min_id = id;

        CREATE TABLE #to_update (
            id INT NOT NULL,
            merged_min_id INT NOT NULL
        )

        DECLARE @todo_intra INT
        SET @todo_intra = 1
        DECLARE @todo_inter INT
        SET @todo_inter = 1
        DECLARE @iterations INT
        SET @iterations = 0

        WHILE ((@todo_intra > 0 OR @todo_inter > 0) AND @iterations < 20)
        BEGIN

            INSERT INTO #to_update (id, merged_min_id)
            SELECT a2.id, MIN(a1.merged_min_id)
            FROM #merging_participants a1
            JOIN #merging_participants a2
                ON a2.database_name = a1.database_name
                AND a2.redcap_project_id = a1.redcap_project_id
                AND a2.record = a1.record
                AND a2.merged_min_id > a1.merged_min_id
            GROUP BY a2.id

            CREATE NONCLUSTERED INDEX #idx_to_update ON #to_update(id)

            UPDATE a
            SET a.merged_min_id = u.merged_min_id
            FROM #merging_participants a
            JOIN #to_update u
                ON u.id = a.id

            SELECT @todo_intra = COUNT(*) FROM #to_update

            DROP INDEX #idx_to_update ON #to_update
            TRUNCATE TABLE #to_update

            INSERT INTO #to_update (id, merged_min_id)
            SELECT a2.id, MIN(a1.merged_min_id)
            FROM #merging_participants a1
            JOIN #merging_participants a2
                ON a2.identifier = a1.identifier
                AND a2.type_id = a1.type_id
                AND a2.merged_min_id > a1.merged_min_id
            GROUP BY a2.id

            CREATE NONCLUSTERED INDEX #idx_to_update ON #to_update(id)

            UPDATE a
            SET a.merged_min_id = u.merged_min_id
            FROM #merging_participants a
            JOIN #to_update u
            ON u.id = a.id

            SELECT @todo_inter = COUNT(*) FROM #to_update

            DROP INDEX #idx_to_update ON #to_update
            TRUNCATE TABLE #to_update

            SET @iterations = @iterations + 1
        END

        INSERT INTO redcap_project_participant_identifier (database_name, redcap_project_id, record, cfg_participant_id_type_id, identifier, merged_participant_id)
        SELECT
            database_name,
            redcap_project_id,
            record,
            type_id,
            identifier,
            merged_min_id
		FROM #merging_participants
        ;

        DROP TABLE #to_update;
        DROP TABLE #merging_participants;
