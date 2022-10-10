CREATE OR ALTER VIEW dq__redcap_projects_without_id AS

    SELECT datalake_database_name, project_id 
    FROM merged__redcap_project crp 

    EXCEPT

    SELECT DISTINCT database_name, redcap_project_id 
    FROM redcap_project_participant_identifier;