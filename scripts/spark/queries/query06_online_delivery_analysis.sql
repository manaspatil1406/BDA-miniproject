SELECT
                location,
                COUNT(*) AS total_restaurants,
                SUM(
                    CASE
                        WHEN online_order_bool = TRUE THEN 1
                        ELSE 0
                    END
                ) AS online_order_restaurants,
                ROUND(
                    100.0 * SUM(
                        CASE
                            WHEN online_order_bool = TRUE THEN 1
                            ELSE 0
                        END
                    ) / COUNT(*),
                    2
                ) AS online_order_percentage
            FROM restaurants
            WHERE location IS NOT NULL
              AND TRIM(location) <> ''
            GROUP BY location
            ORDER BY online_order_percentage DESC;
