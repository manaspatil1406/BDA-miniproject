USE foodieanalytics;

SELECT location,
       COUNT(*) AS total_restaurants
FROM restaurants
WHERE location IS NOT NULL
  AND TRIM(location) <> ''
GROUP BY location
ORDER BY total_restaurants DESC;
