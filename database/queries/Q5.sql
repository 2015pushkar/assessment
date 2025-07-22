/* What clinical data was collected in the last 30 days? */
SELECT *
FROM   clinical_measurements
WHERE  "timestamp" >= CURRENT_DATE - INTERVAL '552 days' -- current date - 16th jan 2024 = 552 days
ORDER  BY "timestamp";  