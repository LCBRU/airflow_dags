IF OBJECT_ID(N'etl_audit_group_type') IS NULL
BEGIN
    CREATE TABLE etl_audit_group_type (
        id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        name NVARCHAR(200) NOT NULL INDEX ix_etl_audit_group_type__name NONCLUSTERED,
    )
    ;

    INSERT INTO etl_audit_group_type(name) VALUES
    ('CiviCRM Case'),
    ('CiviCRM Case Type'),
    ('CiviCRM Contact'),
    ('OpenSpecimen Collection Protocol'),
    ('OpenSpecimen participant'),
    ('REDCap Project'),
    ('REDCap Participant'),
    ('table'),
    ('record');
END
