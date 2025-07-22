/* Q2. What are the glucose trends for a specific participant over time? */
/* raw table */
SELECT
    DATE_TRUNC('day', "timestamp")::date AS day,
    AVG(value_num)                       AS avg_glucose_mg_dl
FROM   clinical_measurements
WHERE  participant_id   = 'P001'
  AND  measurement_type = 'glucose'
GROUP  BY DATE_TRUNC('day', "timestamp")::date   -- same expression, not the alias
ORDER  BY day;


/* aggregated table */
SELECT
    agg_day AS day,
    SUM(avg_value * measurement_count) / SUM(measurement_count) AS avg_glucose_mg_dl
FROM   measurement_aggregations
WHERE  participant_id   = 'P001'
  AND  measurement_type = 'glucose'
GROUP  BY agg_day
ORDER  BY agg_day;