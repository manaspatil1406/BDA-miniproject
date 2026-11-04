SELECT
                name AS restaurant_name,
                location,
                ROUND(AVG(rating_num), 2) AS average_rating,
                MAX(votes_num) AS votes
            FROM restaurants
            WHERE name IS NOT NULL
              AND rating_num BETWEEN 1 AND 5
            GROUP BY name, location
            ORDER BY average_rating DESC, votes DESC
            LIMIT 10;
