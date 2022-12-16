SELECT cosm.collection_protocol_id, cosm.cfg_study_id
FROM warehouse_config.dbo.cfg_openspecimen_study_mapping cosm
WHERE cosm.collection_protocol_id NOT IN (
	SELECT ocp.identifier
	FROM warehouse_central.dbo.openspecimen__collection_protocol ocp
)
;
