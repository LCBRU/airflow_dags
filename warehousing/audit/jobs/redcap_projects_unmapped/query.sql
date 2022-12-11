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
    
    SELECT cri.datalake_database, crm.redcap_project_id
    FROM cfg_redcap_mapping crm
    JOIN cfg_redcap_instance cri
    	ON cri.id = crm.cfg_redcap_instance_id 
) o ON o.datalake_database = crp.datalake_database
    AND o.project_id = crp.project_id
;