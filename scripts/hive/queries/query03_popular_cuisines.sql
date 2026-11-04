USE foodieanalytics;

SELECT TRIM(cuisine) AS cuisine,
       COUNT(*) AS total_restaurants
FROM restaurants
LATERAL VIEW EXPLODE(SPLIT(cuisines, ',')) cuisine_table AS cuisine
WHERE cuisines IS NOT NULL
  AND TRIM(cuisine) <> ''
GROUP BY TRIM(cuisine)
ORDER BY total_restaurants DESC
LIMIT 10;
