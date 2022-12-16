IF OBJECT_ID(N'etl_audit') IS NULL
BEGIN
    CREATE TABLE etl_audit (
        id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        run_id INT NOT NULL INDEX ix_etl_errors__run_id NONCLUSTERED,
        group_id NVARCHAR(500) NOT NULL INDEX ix_etl_audit__group_id NONCLUSTERED,
        group_type_id INT NOT NULL INDEX ix_etl_audit__group_type_id NONCLUSTERED,
        cfg_participant_source_id INT NULL INDEX ix_etl_audit__cfg_participant_source_id NONCLUSTERED,
        cfg_study_id INT NULL INDEX ix_etl_audit__cfg_study_id NONCLUSTERED,
        database_id INT NOT NULL INDEX ix_etl_audit__database_id NONCLUSTERED,
        table_id INT NOT NULL INDEX ix_etl_audit__table_id NONCLUSTERED,
        count_type_id INT NOT NULL INDEX ix_etl_audit__count_type_id NONCLUSTERED,
        count INT NOT NULL,
        FOREIGN KEY (run_id) REFERENCES etl_run(id),
        FOREIGN KEY (group_type_id) REFERENCES etl_audit_group_type(id),
        FOREIGN KEY (cfg_participant_source_id) REFERENCES cfg_participant_source(id),
        FOREIGN KEY (cfg_study_id) REFERENCES cfg_study(id),
        FOREIGN KEY (count_type_id) REFERENCES etl_audit_group_type(id),
        FOREIGN KEY (database_id) REFERENCES etl_audit_database(id),
        FOREIGN KEY (table_id) REFERENCES etl_audit_table(id),
    )
    ;
END
