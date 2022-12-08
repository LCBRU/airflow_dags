CREATE OR ALTER VIEW dq__redcap_projects_without_id AS

    SELECT datalake_database, project_id 
    FROM merged__redcap_project crp 

    EXCEPT

    SELECT DISTINCT mri.datalake_database , mrp.redcap_project_id  
    FROM redcap_project_participant_identifier rppi 
    JOIN meta__redcap_project mrp 
        ON mrp.id = rppi.meta__redcap_project_id
    JOIN meta__redcap_instance mri 
        ON mri.id = mrp.meta__redcap_instance_id 
    ;