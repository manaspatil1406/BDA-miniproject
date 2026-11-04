USE foodieanalytics;

SELECT name AS restaurant_name,
       location,
       CAST(votes AS BIGINT) AS total_votes
FROM restaurants
WHERE votes IS NOT NULL
  AND TRIM(votes) <> ''
  AND CAST(votes AS BIGINT) >= 0
ORDER BY total_votes DESC
LIMIT 10;
