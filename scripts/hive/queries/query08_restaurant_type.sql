USE foodieanalytics;

SELECT rest_type,
       COUNT(*) AS total_restaurants
FROM restaurants
WHERE rest_type IS NOT NULL
  AND TRIM(rest_type) <> ''
GROUP BY rest_type
ORDER BY total_restaurants DESC
LIMIT 15;
