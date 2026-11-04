SELECT
                price_category,
                COUNT(*) AS total_restaurants
            FROM restaurants
            GROUP BY price_category
            ORDER BY total_restaurants DESC;
