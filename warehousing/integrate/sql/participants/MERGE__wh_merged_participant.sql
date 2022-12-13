UPDATE wh_merged_participant SET merged_participant_id = id;

ALTER TABLE wh_merged_participant ALTER COLUMN merged_participant_id INT NOT NULL;

CREATE INDEX idx__wh_merged_participant__merged_participant_id ON wh_merged_participant(merged_participant_id);

CREATE TABLE #to_update (
    id INT NOT NULL,
    merged_participant_id INT NOT NULL
)

DECLARE @todo_intra INT
SET @todo_intra = 1
DECLARE @todo_inter INT
SET @todo_inter = 1
DECLARE @iterations INT
SET @iterations = 0

WHILE ((@todo_intra > 0 OR @todo_inter > 0) AND @iterations < 20)
BEGIN

    INSERT INTO #to_update (id, merged_participant_id)
    SELECT a2.id, MIN(a1.merged_participant_id)
    FROM wh_merged_participant a1
    JOIN wh_merged_participant a2
        ON a2.source_identifier = a1.source_identifier
        AND a2.cfg_participant_source_id = a1.cfg_participant_source_id
        AND a2.merged_participant_id > a1.merged_participant_id
    GROUP BY a2.id

    CREATE NONCLUSTERED INDEX #idx_to_update ON #to_update(id)

    UPDATE a
    SET a.merged_participant_id = u.merged_participant_id
    FROM wh_merged_participant a
    JOIN #to_update u
        ON u.id = a.id

    SELECT @todo_intra = COUNT(*) FROM #to_update

    DROP INDEX #idx_to_update ON #to_update
    TRUNCATE TABLE #to_update

    INSERT INTO #to_update (id, merged_participant_id)
    SELECT a2.id, MIN(a1.merged_participant_id)
    FROM wh_merged_participant a1
    JOIN wh_merged_participant a2
        ON a2.identifier = a1.identifier
        AND a2.cfg_participant_identifier_type_id = a1.cfg_participant_identifier_type_id
        AND a2.merged_participant_id > a1.merged_participant_id
    GROUP BY a2.id

    CREATE NONCLUSTERED INDEX #idx_to_update ON #to_update(id)

    UPDATE a
    SET a.merged_participant_id = u.merged_participant_id
    FROM wh_merged_participant a
    JOIN #to_update u
    ON u.id = a.id

    SELECT @todo_inter = COUNT(*) FROM #to_update

    DROP INDEX #idx_to_update ON #to_update
    TRUNCATE TABLE #to_update

    SET @iterations = @iterations + 1
END

DROP TABLE #to_update;
