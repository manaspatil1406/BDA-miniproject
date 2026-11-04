
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, regexp_extract, regexp_replace,
    when, count, avg, desc
)

# --------------------------------------------------
# 1. CREATE SPARK SESSION
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("FoodieAnalytics_DataFrame_Transformations")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

DATASET_PATH = (
    "/mnt/c/BDA-miniproject/"
    "dataset/processed/hadoop_safe_zomato.csv"
)

# --------------------------------------------------
# 2. LOAD DATASET
# --------------------------------------------------

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

print("\n========== ORIGINAL DATASET ==========")
print("Total records:", df.count())

# --------------------------------------------------
# 3. CONVERT STRING COLUMNS INTO USEFUL TYPES
# --------------------------------------------------

# Extract numeric rating from values such as 4.1/5.
# NEW, -, and empty values become NULL.

df = df.withColumn(
    "rating_num",
    regexp_extract(
        trim(col("rate")),
        r"^([0-5](?:\.[0-9]+)?)",
        1
    ).cast("double")
)

# Convert votes into BIGINT.

df = df.withColumn(
    "votes_num",
    regexp_replace(trim(col("votes")), ",", "").cast("long")
)

# Remove commas from cost and convert to DOUBLE.

df = df.withColumn(
    "cost_for_two_num",
    regexp_extract(
        regexp_replace(trim(col("approx_cost_for_two_people")), ",", ""),
        r"([0-9]+(?:\.[0-9]+)?)",
        1
    ).cast("double")
)

# Convert online ordering into Boolean.

df = df.withColumn(
    "online_order_bool",
    when(lower(trim(col("online_order"))) == "true", True)
    .when(lower(trim(col("online_order"))) == "false", False)
    .otherwise(None)
)

# Convert table booking into Boolean.

df = df.withColumn(
    "book_table_bool",
    when(lower(trim(col("book_table"))) == "true", True)
    .when(lower(trim(col("book_table"))) == "false", False)
    .otherwise(None)
)

# --------------------------------------------------
# 4. CREATE RATING CATEGORIES
# --------------------------------------------------

df = df.withColumn(
    "rating_category",
    when(col("rating_num") >= 4.5, "Excellent")
    .when(col("rating_num") >= 4.0, "Very Good")
    .when(col("rating_num") >= 3.0, "Good")
    .when(col("rating_num") >= 2.0, "Average")
    .when(col("rating_num").isNotNull(), "Below Average")
    .otherwise("Not Rated")
)

# --------------------------------------------------
# 5. CREATE PRICE CATEGORIES
# --------------------------------------------------

df = df.withColumn(
    "price_category",
    when(col("cost_for_two_num").isNull(), "Unknown")
    .when(col("cost_for_two_num") <= 500, "Budget")
    .when(col("cost_for_two_num") <= 1000, "Moderate")
    .when(col("cost_for_two_num") <= 2000, "Expensive")
    .otherwise("Premium")
)

# --------------------------------------------------
# 6. DISPLAY UPDATED SCHEMA
# --------------------------------------------------

print("\n========== UPDATED DATAFRAME SCHEMA ==========")
df.printSchema()

# --------------------------------------------------
# 7. SELECT OPERATION
# --------------------------------------------------

print("\n========== SELECT OPERATION ==========")

df.select(
    "name",
    "location",
    "rating_num",
    "votes_num",
    "cost_for_two_num"
).show(10, truncate=False)

# --------------------------------------------------
# 8. FILTER OPERATION
# --------------------------------------------------

print("\n========== RESTAURANTS WITH RATING >= 4 ==========")

df.filter(
    col("rating_num") >= 4.0
).select(
    "name", "location", "rating_num"
).show(10, truncate=False)

# --------------------------------------------------
# 9. GROUP BY AND COUNT
# --------------------------------------------------

print("\n========== RESTAURANT COUNT BY LOCATION ==========")

df.groupBy("location").agg(
    count("*").alias("total_restaurants")
).orderBy(
    desc("total_restaurants")
).show(10, truncate=False)

# --------------------------------------------------
# 10. AVERAGE RATING BY LOCATION
# --------------------------------------------------

print("\n========== AVERAGE RATING BY LOCATION ==========")

df.filter(
    col("rating_num").isNotNull()
).groupBy("location").agg(
    avg("rating_num").alias("average_rating")
).orderBy(
    desc("average_rating")
).show(10, truncate=False)

# --------------------------------------------------
# 11. ONLINE ORDERING FILTER
# --------------------------------------------------

print("\n========== ONLINE ORDERING RESTAURANTS ==========")

df.filter(
    col("online_order_bool") == True
).select(
    "name", "location", "online_order_bool"
).show(10, truncate=False)

# --------------------------------------------------
# 12. RATING CATEGORY DISTRIBUTION
# --------------------------------------------------

print("\n========== RATING CATEGORY DISTRIBUTION ==========")

df.groupBy("rating_category").agg(
    count("*").alias("total_restaurants")
).orderBy(
    desc("total_restaurants")
).show(truncate=False)

# --------------------------------------------------
# 13. PRICE CATEGORY DISTRIBUTION
# --------------------------------------------------

print("\n========== PRICE CATEGORY DISTRIBUTION ==========")

df.groupBy("price_category").agg(
    count("*").alias("total_restaurants")
).orderBy(
    desc("total_restaurants")
).show(truncate=False)

# --------------------------------------------------
# 14. DATA QUALITY SUMMARY
# --------------------------------------------------

print("\n========== DATA QUALITY SUMMARY ==========")

df.select(
    count("*").alias("total_records"),
    count("rating_num").alias("valid_ratings"),
    count("votes_num").alias("valid_votes"),
    count("cost_for_two_num").alias("valid_cost_values")
).show()

print("\n========== TRANSFORMATIONS COMPLETED ==========")

spark.stop()
