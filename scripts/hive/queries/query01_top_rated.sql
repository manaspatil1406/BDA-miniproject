USE foodieanalytics;

SELECT name AS restaurant_name,
       location,
       CAST(rate AS DOUBLE) AS rating
FROM restaurants
WHERE rate IS NOT NULL
  AND TRIM(rate) NOT IN ('', 'NEW', '-')
  AND CAST(rate AS DOUBLE) BETWEEN 1 AND 5
ORDER BY rating DESC
LIMIT 10;
