SELECT
                location,
                ROUND(AVG(cost_for_two_num), 2)
                    AS average_cost_for_two,
                COUNT(cost_for_two_num) AS valid_cost_records
            FROM restaurants
            WHERE location IS NOT NULL
              AND cost_for_two_num > 0
            GROUP BY location
            ORDER BY average_cost_for_two DESC;
