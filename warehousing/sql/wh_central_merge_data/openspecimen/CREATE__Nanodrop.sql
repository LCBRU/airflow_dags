CREATE TABLE openspecimen__nanodrop (
    specimen_identifier INT NOT NULL UNIQUE,
    plate_id VARCHAR(255),
    well VARCHAR(255),
    time_stamp DATETIME2,
    conc NUMERIC(19,6),
    a260 NUMERIC(19,6),
    a280 NUMERIC(19,6),
    a260_280 NUMERIC(19,6),
    a260_230 NUMERIC(19,6),
    factor_ng_per_ul NUMERIC(19,6),
    cursor_pos NUMERIC(19,6),
    cursor_abs VARCHAR(255)
)

INSERT INTO openspecimen__nanodrop (specimen_identifier, plate_id, well, time_stamp, conc, a260, a280, a260_280, a260_230, factor_ng_per_ul, cursor_pos, cursor_abs)
SELECT
	cfre.object_id AS specimen_identifier,
	de_a_1 AS plate_id,
	de_a_2 AS well,
	de_a_4 AS time_stamp,
	de_a_5 AS conc,
	de_a_7 AS a260,
	de_a_8 AS a280,
	de_a_9 AS a260_280,
	de_a_10 AS a260_230,
	de_a_11 AS factor_ng_per_ul,
	de_a_12 AS cursor_pos,
	de_a_13 AS cursor_abs
FROM datalake_openspecimen.dbo.de_e_11001 de
JOIN datalake_openspecimen.dbo.catissue_form_record_entry cfre
	ON cfre.record_id = de.identifier
	AND cfre.activity_status = 'active'
JOIN datalake_openspecimen.dbo.catissue_form_context cfc
	ON cfc.identifier = cfre.form_ctxt_id
	AND cfc.entity_type IN ('specimen','specimenevent')
	AND cfc.deleted_on IS NULL
;

