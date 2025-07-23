/* Q4. Which measurements have quality scores below our threshold? */
/* Any quality_score < 0.95 will flag as low quality */
SELECT *
FROM   measurement_aggregations
WHERE  low_quality_count > 0       
ORDER  BY agg_day, study_id, participant_id;
