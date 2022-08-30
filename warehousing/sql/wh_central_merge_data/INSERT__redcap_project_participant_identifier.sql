IF OBJECT_ID(N'dbo.redcap_project_participant_identifier', N'U') IS NOT NULL  
    DROP TABLE dbo.redcap_project_participant_identifier;

CREATE TABLE warehouse_central.dbo.redcap_project_participant_identifier (
  id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
  database_name NVARCHAR(500) NOT NULL,
  redcap_project_id INT NOT NULL,
  record NVARCHAR(100) NOT NULL,
  cfg_participant_id_type_id INT NOT NULL,
  identifier NVARCHAR(100) NOT NULL
);

CREATE INDEX idx__redcap_project_participant_identifier__db_proj_record
    ON warehouse_central.dbo.redcap_project_participant_identifier(database_name, redcap_project_id, record)
;

CREATE INDEX idx__redcap_project_participant_identifier__cfg_participant_id_type_id_identifier
    ON warehouse_central.dbo.redcap_project_participant_identifier(cfg_participant_id_type_id, identifier)
;


EXEC sp_MSforeachdb
@command1='IF ''?'' LIKE ''datalake_redcap_%''
BEGIN
    INSERT INTO redcap_project_participant_identifier (database_name, redcap_project_id, record, cfg_participant_id_type_id, identifier)
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
