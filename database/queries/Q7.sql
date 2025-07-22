/* What's the average BMI for participants in a specific study? */

/* raw table */
WITH latest_weight AS (                      -- last weight per participant
    SELECT DISTINCT ON (participant_id)
           participant_id,
           value_num      AS weight_kg
    FROM   clinical_measurements
    WHERE  study_id = 'STUDY001'
      AND  measurement_type = 'weight'
    ORDER  BY participant_id, "timestamp" DESC
), latest_height AS (                        -- last height per participant
    SELECT DISTINCT ON (participant_id)
           participant_id,
           value_num      AS height_cm
    FROM   clinical_measurements
    WHERE  study_id = 'STUDY001'
      AND  measurement_type = 'height'
    ORDER  BY participant_id, "timestamp" DESC
), joined AS (                               -- only participants who have both
    SELECT w.participant_id,
           w.weight_kg,
           h.height_cm
    FROM   latest_weight  w
    JOIN   latest_height  h USING (participant_id)
)
SELECT
    ROUND( AVG(weight_kg / POWER(height_cm/100, 2))::numeric, 2 )
FROM   joined;


/* aggregated table */
WITH latest AS (                             -- last daily bucket per participant
    SELECT DISTINCT ON (participant_id, measurement_type)
           participant_id,
           measurement_type,
           avg_value   AS value_num,
           agg_day
    FROM   measurement_aggregations
    WHERE  study_id = 'STUDY001'
      AND  measurement_type IN ('weight','height')
    ORDER  BY participant_id, measurement_type, agg_day DESC
), pivot AS (                                -- pivot weight & height side-by-side
    SELECT  MAX(CASE WHEN measurement_type = 'weight'  THEN value_num END) AS weight_kg,
            MAX(CASE WHEN measurement_type = 'height'  THEN value_num END) AS height_cm
    FROM    latest
    GROUP   BY participant_id
    HAVING  COUNT(*) = 2                     -- keep only if both measurements
)
SELECT
    ROUND( AVG(weight_kg / POWER(height_cm/100, 2))::numeric, 2 )
FROM   pivot;
