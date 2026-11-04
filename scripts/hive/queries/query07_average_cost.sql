USE foodieanalytics;

SELECT location,
       ROUND(AVG(CAST(approx_cost_for_two_people AS DOUBLE)), 2)
           AS average_cost_for_two
FROM restaurants
WHERE approx_cost_for_two_people IS NOT NULL
  AND TRIM(approx_cost_for_two_people) <> ''
  AND CAST(approx_cost_for_two_people AS DOUBLE) > 0
GROUP BY location
ORDER BY average_cost_for_two DESC;
