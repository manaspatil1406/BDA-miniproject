USE foodieanalytics;

SELECT location,
       COUNT(*) AS online_order_restaurants
FROM restaurants
WHERE LOWER(TRIM(online_order)) = 'true'
GROUP BY location
ORDER BY online_order_restaurants DESC;
