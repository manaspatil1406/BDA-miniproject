USE foodieanalytics;

SELECT location,
       ROUND(AVG(CAST(rate AS DOUBLE)), 2) AS average_rating,
       COUNT(rate) AS rated_restaurant_count
FROM restaurants
WHERE rate IS NOT NULL
  AND TRIM(rate) NOT IN ('', 'NEW', '-')
  AND CAST(rate AS DOUBLE) BETWEEN 1 AND 5
GROUP BY location
ORDER BY average_rating DESC;
