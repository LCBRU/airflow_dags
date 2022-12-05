SELECT crp.*
FROM merged__redcap_project crp
JOIN (
    SELECT datalake_database, project_id
    FROM merged__redcap_project crp
    WHERE crp.status = 1
    
    -- 0 = Development
    -- 1 = Online
    -- 2 = ?
    -- 3 = Archived
    
    EXCEPT
    
    SELECT datalake_database, redcap_project_id
    FROM etl__redcap_project_mapping
) o ON o.datalake_database = crp.datalake_database
    AND o.project_id = crp.project_id
;