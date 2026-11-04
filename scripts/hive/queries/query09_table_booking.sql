USE foodieanalytics;

SELECT location,
       COUNT(*) AS table_booking_restaurants
FROM restaurants
WHERE LOWER(TRIM(book_table)) = 'true'
GROUP BY location
ORDER BY table_booking_restaurants DESC;
