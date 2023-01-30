SET QUOTED_IDENTIFIER OFF;
	
CREATE TABLE dbo.redcap_file (
    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    cfg_redcap_instance_id INT NOT NULL,
    doc_id INT NOT NULL,
    stored_name NVARCHAR(500) NOT NULL,
    mime_type NVARCHAR(500) NOT NULL,
    doc_name NVARCHAR(500) NOT NULL,
    doc_size INT NOT NULL,
    file_extension NVARCHAR(500) NOT NULL,
    gzipped BIT NOT NULL,
    INDEX idx__meta__redcap_file__doc_name (doc_name),
    UNIQUE (cfg_redcap_instance_id, doc_id),
    UNIQUE (cfg_redcap_instance_id, stored_name),
);

EXEC sp_MSforeachdb
@command1="IF '?' LIKE 'datalake_redcap_%'
BEGIN 
	INSERT INTO warehouse_central.dbo.redcap_file (cfg_redcap_instance_id, doc_id, stored_name, mime_type, doc_name, doc_size, file_extension, gzipped)
    SELECT DISTINCT
        mri.id AS cfg_redcap_instance_id,
        rem.doc_id,
        rem.stored_name,
        rem.mime_type,
        rem.doc_name,
        rem.doc_size,
        rem.file_extension,
        rem.gzipped
    FROM [?].dbo.redcap_edocs_metadata rem
    JOIN warehouse_config.dbo.cfg_redcap_instance mri
        ON mri.datalake_database = '?'
    JOIN warehouse_config.dbo.cfg_redcap_mapping rm
        ON rm.cfg_redcap_instance_id = mri.id
        AND rm.redcap_project_id = rem.project_id
        AND rm.cfg_study_id > 0

END"

SET QUOTED_IDENTIFIER ON;
