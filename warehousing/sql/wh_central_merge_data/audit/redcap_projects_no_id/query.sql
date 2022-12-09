SELECT *
FROM merged__redcap_project mrp 
JOIN (
	SELECT
    	datalake_database,
    	project_id
    FROM merged__redcap_project crp 

    EXCEPT

    SELECT DISTINCT mri.datalake_database , mrp.redcap_project_id  
    FROM redcap_project_participant_identifier rppi 
    JOIN meta__redcap_project mrp 
        ON mrp.id = rppi.meta__redcap_project_id
    JOIN cfg_redcap_instance mri 
        ON mri.id = mrp.cfg_redcap_instance_id 
) no_id
	ON no_id.datalake_database = mrp.datalake_database
	AND no_id.project_id = mrp.project_id
	;