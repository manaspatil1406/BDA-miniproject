SELECT
                TRIM(cuisine) AS cuisine,
                COUNT(*) AS restaurant_count
            FROM restaurants
            LATERAL VIEW EXPLODE(
                SPLIT(cuisines, ',')
            ) cuisine_table AS cuisine
            WHERE cuisines IS NOT NULL
              AND TRIM(cuisine) <> ''
            GROUP BY TRIM(cuisine)
            ORDER BY restaurant_count DESC
            LIMIT 20;
