CREATE VIEW desc__openspecimen AS
SELECT os.*
FROM warehouse_central.dbo.desc__openspecimen os
JOIN warehouse_central.dbo.etl__openspecimen_mapping eom
	ON eom.collection_protocol_id = os.collection_protocol_identifier
WHERE eom.study_name = %(study_name)s
;