
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    regexp_extract,
    regexp_replace,
    split,
    when,
    size,
    array_distinct,
    count,
    sum as spark_sum
)

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("FoodieAnalytics_Recommendation_Data_Preparation")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n==========================================")
print("FOODIEANALYTICS - DATA PREPARATION")
print("==========================================")

# --------------------------------------------------
# 2. Define Dataset Paths
# --------------------------------------------------

input_path = (
    "/mnt/c/BDA-miniproject/"
    "dataset/processed/hadoop_safe_zomato.csv"
)

output_path = (
    "/mnt/c/BDA-miniproject/"
    "dataset/processed/recommendation_ready"
)

# --------------------------------------------------
# 3. Load Dataset
# --------------------------------------------------

print("\nLoading restaurant dataset...")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .option("multiLine", False)
    .option("quote", '"')
    .option("escape", '"')
    .option("maxCharsPerColumn", 10000000)
    .csv(input_path)
)

print("Dataset loaded successfully.")

# --------------------------------------------------
# 4. Convert Numerical Columns
# --------------------------------------------------

print("\nPreparing numerical features...")

df = (
    df
    .withColumn(
        "rating_num",
        regexp_extract(
            trim(col("rate")),
            r"^([0-5](?:\.[0-9])?)",
            1
        ).cast("double")
    )
    .withColumn(
        "votes_num",
        regexp_replace(trim(col("votes")), ",", "").cast("long")
    )
    .withColumn(
        "cost_for_two_num",
        regexp_extract(
            regexp_replace(
                trim(col("approx_cost_for_two_people")),
                ",",
                ""
            ),
            r"([0-9]+(?:\.[0-9]+)?)",
            1
        ).cast("double")
    )
)

# --------------------------------------------------
# 5. Convert Boolean Columns
# --------------------------------------------------

print("Preparing Boolean features...")

df = (
    df
    .withColumn(
        "online_order_bool",
        when(lower(trim(col("online_order"))) == "yes", True)
        .when(lower(trim(col("online_order"))) == "no", False)
        .otherwise(None)
    )
    .withColumn(
        "book_table_bool",
        when(lower(trim(col("book_table"))) == "yes", True)
        .when(lower(trim(col("book_table"))) == "no", False)
        .otherwise(None)
    )
)

# --------------------------------------------------
# 6. Prepare Cuisine and Location Features
# --------------------------------------------------

print("Preparing cuisine and location features...")

df = (
    df
    .withColumn(
        "cuisine_list",
        when(
            col("cuisines").isNotNull()
            & (trim(col("cuisines")) != ""),
            array_distinct(
                split(
                    lower(trim(col("cuisines"))),
                    r"\s*,\s*"
                )
            )
        )
        .otherwise(None)
    )
    .withColumn(
        "location_clean",
        when(
            col("location").isNotNull(),
            trim(col("location"))
        )
        .otherwise(None)
    )
)

# --------------------------------------------------
# 7. Remove Invalid Numerical Values
# --------------------------------------------------

df = (
    df
    .withColumn(
        "rating_num",
        when(
            (col("rating_num") >= 0)
            & (col("rating_num") <= 5),
            col("rating_num")
        ).otherwise(None)
    )
    .withColumn(
        "cost_for_two_num",
        when(
            col("cost_for_two_num") > 0,
            col("cost_for_two_num")
        ).otherwise(None)
    )
    .withColumn(
        "votes_num",
        when(
            col("votes_num") >= 0,
            col("votes_num")
        ).otherwise(None)
    )
)

# --------------------------------------------------
# 8. Display Dataset Information
# --------------------------------------------------

print("\n==========================================")
print("PREPARED DATASET INFORMATION")
print("==========================================")

total_records = df.count()

print("Total restaurant records:", total_records)
print("Total columns:", len(df.columns))

print("\nPrepared Schema:")
df.printSchema()

# --------------------------------------------------
# 9. Data Quality Summary
# --------------------------------------------------

print("\nData Quality Summary:")

df.select(
    count("*").alias("total_records"),
    spark_sum(
        when(col("rating_num").isNotNull(), 1).otherwise(0)
    ).alias("valid_ratings"),
    spark_sum(
        when(col("votes_num").isNotNull(), 1).otherwise(0)
    ).alias("valid_votes"),
    spark_sum(
        when(col("cost_for_two_num").isNotNull(), 1).otherwise(0)
    ).alias("valid_cost_values"),
    spark_sum(
        when(col("cuisine_list").isNotNull(), 1).otherwise(0)
    ).alias("valid_cuisine_values"),
    spark_sum(
        when(col("location_clean").isNotNull(), 1).otherwise(0)
    ).alias("valid_locations")
).show(truncate=False)

# --------------------------------------------------
# 10. Display Sample Records
# --------------------------------------------------

print("\nSample Prepared Restaurant Records:")

df.select(
    "name",
    "location_clean",
    "cuisines",
    "cuisine_list",
    "rating_num",
    "votes_num",
    "cost_for_two_num",
    "online_order_bool"
).show(10, truncate=False)

# --------------------------------------------------
# 11. Save Prepared Dataset in Parquet Format
# --------------------------------------------------

print("\nSaving recommendation-ready dataset...")

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)

print("Prepared dataset saved successfully.")
print("Output location:", output_path)

# --------------------------------------------------
# 12. Stop Spark Session
# --------------------------------------------------

spark.stop()

print("\n==========================================")
print("STEP 2 COMPLETED SUCCESSFULLY")
print("==========================================")
