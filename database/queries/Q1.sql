/*Q1. Which studies have the highest data quality scores?*/
SELECT
    study_id,
    ROUND(AVG(quality_score), 3) AS avg_qs,
    COUNT(*) AS rows
FROM
    clinical_measurements
GROUP BY
    study_id
ORDER BY
    avg_qs DESC;


SELECT
    study_id,
    AVG(avg_quality_score) AS avg_qs,
    COUNT(*) AS rows
FROM   measurement_aggregations
GROUP  BY study_id
ORDER  BY avg_qs DESC;



