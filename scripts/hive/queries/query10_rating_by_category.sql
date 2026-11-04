USE foodieanalytics;

SELECT listed_in_type,
       ROUND(AVG(CAST(rate AS DOUBLE)), 2) AS average_rating,
       COUNT(*) AS rated_records
FROM restaurants
WHERE listed_in_type IS NOT NULL
  AND TRIM(listed_in_type) <> ''
  AND rate IS NOT NULL
  AND TRIM(rate) NOT IN ('', 'NEW', '-')
  AND CAST(rate AS DOUBLE) BETWEEN 1 AND 5
GROUP BY listed_in_type
ORDER BY average_rating DESC;
