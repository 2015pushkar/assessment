/* How many participants are enrolled in each study? */
SELECT
    study_id,
    COUNT(*) AS participants
FROM   participant_enrollments
GROUP  BY study_id
ORDER  BY participants DESC;