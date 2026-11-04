SELECT
                COUNT(*) AS valid_record_count,
                ROUND(
                    CORR(
                        rating_num,
                        CAST(votes_num AS DOUBLE)
                    ),
                    4
                ) AS rating_votes_correlation
            FROM restaurants
            WHERE rating_num BETWEEN 1 AND 5
              AND votes_num IS NOT NULL;
