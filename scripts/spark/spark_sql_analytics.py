
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, regexp_extract,
    regexp_replace, when
)
from pathlib import Path

# ==================================================
# 1. CREATE SPARK SESSION
# ==================================================

spark = (
    SparkSession.builder
    .appName("FoodieAnalytics_SparkSQL")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

PROJECT_DIR = Path("/mnt/c/BDA-miniproject")

DATASET_PATH = str(
    PROJECT_DIR / "dataset/processed/hadoop_safe_zomato.csv"
)

QUERY_DIR = PROJECT_DIR / "scripts/spark/queries"
RESULT_DIR = PROJECT_DIR / "reports/spark_results"

QUERY_DIR.mkdir(parents=True, exist_ok=True)
RESULT_DIR.mkdir(parents=True, exist_ok=True)

print("\n========================================")
print(" FOODIE ANALYTICS - SPARK SQL ANALYTICS")
print("========================================")

try:

    # ==================================================
    # 2. LOAD AND PREPARE DATA
    # ==================================================

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "false")
        .option("quote", '"')
        .option("escape", '"')
        .option("maxCharsPerColumn", "10000000")
        .csv(DATASET_PATH)
    )

    # Convert rating safely
    df = df.withColumn(
        "rating_num",
        regexp_extract(
            trim(col("rate")),
            r"^([0-5](?:\.[0-9]+)?)",
            1
        ).cast("double")
    )

    # Keep only ratings between 1 and 5
    df = df.withColumn(
        "rating_num",
        when(
            col("rating_num").between(1.0, 5.0),
            col("rating_num")
        )
    )

    # Convert votes
    df = df.withColumn(
        "votes_num",
        regexp_replace(
            trim(col("votes")), ",", ""
        ).cast("long")
    )

    # Convert cost
    df = df.withColumn(
        "cost_for_two_num",
        regexp_extract(
            regexp_replace(
                trim(col("approx_cost_for_two_people")),
                ",", ""
            ),
            r"([0-9]+(?:\.[0-9]+)?)",
            1
        ).cast("double")
    )

    # Convert online order to Boolean
    df = df.withColumn(
        "online_order_bool",
        when(lower(trim(col("online_order"))) == "true", True)
        .when(lower(trim(col("online_order"))) == "false", False)
        .otherwise(None)
    )

    # Create price categories
    df = df.withColumn(
        "price_category",
        when(col("cost_for_two_num").isNull(), "Unknown")
        .when(col("cost_for_two_num") <= 500, "Budget")
        .when(col("cost_for_two_num") <= 1000, "Moderate")
        .when(col("cost_for_two_num") <= 2000, "Expensive")
        .otherwise("Premium")
    )

    # Register temporary SQL view
    df.createOrReplaceTempView("restaurants")

    print("\nDataset loaded successfully.")
    print("Total records:", df.count())
    print("Total columns:", len(df.columns))

    # ==================================================
    # 3. DEFINE EIGHT SPARK SQL QUERIES
    # ==================================================

    queries = {

        "query01_restaurants_by_location": """
            SELECT
                location,
                COUNT(*) AS total_restaurants
            FROM restaurants
            WHERE location IS NOT NULL
              AND TRIM(location) <> ''
            GROUP BY location
            ORDER BY total_restaurants DESC
        """,

        "query02_popular_cuisines": """
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
            LIMIT 20
        """,

        "query03_average_rating_by_location": """
            SELECT
                location,
                ROUND(AVG(rating_num), 2) AS average_rating,
                COUNT(rating_num) AS rated_restaurant_count
            FROM restaurants
            WHERE location IS NOT NULL
              AND rating_num BETWEEN 1 AND 5
            GROUP BY location
            ORDER BY average_rating DESC
        """,

        "query04_top_rated_restaurants": """
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
            LIMIT 10
        """,

        "query05_average_cost_by_location": """
            SELECT
                location,
                ROUND(AVG(cost_for_two_num), 2)
                    AS average_cost_for_two,
                COUNT(cost_for_two_num) AS valid_cost_records
            FROM restaurants
            WHERE location IS NOT NULL
              AND cost_for_two_num > 0
            GROUP BY location
            ORDER BY average_cost_for_two DESC
        """,

        "query06_online_delivery_analysis": """
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
            ORDER BY online_order_percentage DESC
        """,

        "query07_price_range_distribution": """
            SELECT
                price_category,
                COUNT(*) AS total_restaurants
            FROM restaurants
            GROUP BY price_category
            ORDER BY total_restaurants DESC
        """,

        "query08_rating_votes_relationship": """
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
              AND votes_num IS NOT NULL
        """
    }

    # ==================================================
    # 4. EXECUTE QUERIES AND SAVE RESULTS
    # ==================================================

    for query_name, sql_query in queries.items():

        print("\n========================================")
        print("Executing:", query_name)
        print("========================================")

        # Save the SQL query separately
        sql_file = QUERY_DIR / f"{query_name}.sql"
        sql_file.write_text(
            sql_query.strip() + ";\n",
            encoding="utf-8"
        )

        # Execute SQL using Spark
        result = spark.sql(sql_query)

        # Display results
        result.show(20, truncate=False)

        # Save result as CSV
        output_path = RESULT_DIR / query_name

        (
            result.write
            .mode("overwrite")
            .option("header", "true")
            .csv(str(output_path))
        )

        print("Result saved to:", output_path)

    # ==================================================
    # 5. COMPLETION SUMMARY
    # ==================================================

    print("\n========================================")
    print(" ALL EIGHT SPARK SQL QUERIES COMPLETED")
    print("========================================")

    print("SQL files:", QUERY_DIR)
    print("Result files:", RESULT_DIR)

finally:
    spark.stop()
