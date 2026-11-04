SELECT
                location,
                ROUND(AVG(rating_num), 2) AS average_rating,
                COUNT(rating_num) AS rated_restaurant_count
            FROM restaurants
            WHERE location IS NOT NULL
              AND rating_num BETWEEN 1 AND 5
            GROUP BY location
            ORDER BY average_rating DESC;
