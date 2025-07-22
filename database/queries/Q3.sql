/* How do measurement counts compare across different research sites? */
/* raw table */
SELECT
    site_id,
    COUNT(*) AS measurement_count
FROM   clinical_measurements
GROUP  BY site_id
ORDER  BY measurement_count DESC;

/* aggregated table */
SELECT
    site_id,
    SUM(measurement_count) AS measurement_count
FROM   measurement_aggregations
GROUP  BY site_id
ORDER  BY measurement_count DESC;