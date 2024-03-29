CREATE OR ALTER VIEW desc__openspecimen AS
SELECT
	ocp.identifier AS collection_protocol_identifier,
	ocp.title AS collection_protocol_title,
	ocp.short_title AS collection_protocol_short_title,
	ocp.activity_status AS collection_protocol_activity_status,
	op.identifier AS participant_identifier,
	op.empi_id,
	op.activity_status AS participant_activity_status,
	or2.identifier AS registration_identifier,
	or2.protocol_participant_id,
	or2.registration_date,
	or2.activity_status AS registration_activity_status,
	oe.identifier AS event_identifier,
	oe.collection_point_label AS event_collection_point_label,
	oe.activity_status AS event_activity_status,
	osg.identifier AS specimen_group_identifier,
	osg.name AS specimen_group_name,
	osg.comments AS specimen_group_comments,
	osg.collection_status AS specimen_group_collection_status,
	osg.collection_comments AS specimen_group_collection_comments,
	osg.collection_timestamp AS specimen_group_collection_timestamp,
	osg.received_comments AS specimen_group_received_comments,
	osg.received_timestamp AS specimen_group_received_timestamp,
	osg.activity_status specimen_group_activity_status,
	s.identifier AS sample_identifier,
	s.label,
	s.barcode,
	s.comments,
	s.specimen_group_id,
	s.available_quantity,
	s.initial_quantity,
	s.created_on,
	s.activity_status,
	s.collection_status,
	s.specimen_class,
	s.specimen_type,
	s.lineage,
	s.parent_specimen_id,
	on2.plate_id AS nanodrop_plate_id,
	on2.well AS nanodrop_well,
	on2.time_stamp AS nanodrop_timestamp,
	on2.conc AS nanodrop_conc,
	on2.a260 AS nanodrop_a260,
	on2.a280 AS nanodrop_a280,
	on2.a260_280 AS nanodrop_a20_280,
	on2.a260_230 AS nanodrop_a260_230,
	on2.factor_ng_per_ul AS nanodrop_factor_ng_ul,
	on2.cursor_pos AS nanodrop_cursor_pos,
	on2.cursor_abs AS nanodrop_abs 
FROM openspecimen__specimen s
JOIN openspecimen__specimen_group osg
	ON osg.identifier = s.specimen_group_id
JOIN openspecimen__event oe
	ON oe.identifier = osg.event_id
JOIN openspecimen__registration or2
	ON or2.identifier = osg.registration_id
JOIN openspecimen__participant op
	ON op.identifier = or2.participant_id
JOIN openspecimen__collection_protocol ocp
	ON ocp.identifier = or2.collection_protocol_id
LEFT JOIN openspecimen__nanodrop on2 
	ON on2.specimen_identifier = s.identifier
;
