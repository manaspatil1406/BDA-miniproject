
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)
from pyspark.sql.functions import (
    count,
    sum,
    avg,
    round,
    desc
)
import os

# ==========================================
# FOODIEANALYTICS - CONSOLIDATED ANALYTICS
# ==========================================

spark = (
    SparkSession.builder
    .appName("FoodieAnalytics_Consolidated_Analytics")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 1. DEFINE PATHS
# ==========================================

base_path = "/mnt/c/BDA-miniproject"

input_path = (
    base_path + "/dataset/streaming/orders_input"
)

checkpoint_path = (
    base_path + "/dataset/streaming/consolidation_checkpoint"
)

processed_path = (
    base_path + "/reports/streaming/processed_orders"
)

reports_path = (
    base_path + "/reports/streaming"
)

print("\n==========================================")
print(" FOODIEANALYTICS - CONSOLIDATED ANALYTICS")
print("==========================================")

# ==========================================
# 2. DEFINE ORDER SCHEMA
# ==========================================

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("restaurant_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_timestamp", StringType(), True)
])

# ==========================================
# 3. READ ORDER FILES AS A STREAM
# ==========================================

print("\nInitializing streaming input...")

orders_stream = (
    spark.readStream
    .format("json")
    .schema(order_schema)
    .option("maxFilesPerTrigger", 1)
    .load(input_path)
)

# ==========================================
# 4. SAVE PROCESSED MICRO-BATCHES
# ==========================================

def save_batch(batch_df, batch_id):

    print(f"\nSaving micro-batch: {batch_id}")

    batch_df.write \
        .mode("append") \
        .parquet(processed_path)

    print(f"Micro-batch {batch_id} saved successfully.")


# ==========================================
# 5. START STREAMING QUERY
# ==========================================

print("\nStarting streaming data consolidation...")

query = (
    orders_stream.writeStream
    .foreachBatch(save_batch)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

print("\nStreaming data consolidation completed.")

# ==========================================
# 6. LOAD CONSOLIDATED ORDER DATA
# ==========================================

print("\nLoading consolidated order records...")

orders_df = spark.read.parquet(processed_path)

total_orders = orders_df.count()

print("Total processed orders:", total_orders)

# ==========================================
# 7. OVERALL ORDER ANALYTICS
# ==========================================

print("\n==========================================")
print("OVERALL ORDER ANALYTICS")
print("==========================================")

overall_summary = orders_df.agg(
    count("*").alias("total_orders"),
    round(sum("order_amount"), 2).alias("total_revenue"),
    round(avg("order_amount"), 2).alias("average_order_value")
)

overall_summary.show(truncate=False)

overall_summary.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(reports_path + "/overall_summary")

# ==========================================
# 8. RESTAURANT-WISE ANALYTICS
# ==========================================

print("\n==========================================")
print("RESTAURANT-WISE ANALYTICS")
print("==========================================")

restaurant_summary = (
    orders_df.groupBy("restaurant_id")
    .agg(
        count("*").alias("total_orders"),
        round(sum("order_amount"), 2).alias("total_revenue"),
        round(avg("order_amount"), 2).alias("average_order_value")
    )
    .orderBy(desc("total_orders"))
)

restaurant_summary.show(truncate=False)

restaurant_summary.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(reports_path + "/restaurant_summary")

# ==========================================
# 9. ORDER STATUS ANALYTICS
# ==========================================

print("\n==========================================")
print("ORDER STATUS ANALYTICS")
print("==========================================")

status_summary = (
    orders_df.groupBy("order_status")
    .agg(
        count("*").alias("total_orders"),
        round(sum("order_amount"), 2).alias("total_order_value")
    )
    .orderBy(desc("total_orders"))
)

status_summary.show(truncate=False)

status_summary.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(reports_path + "/status_summary")

# ==========================================
# 10. SAVE FINAL ORDER RECORDS
# ==========================================

print("\n==========================================")
print("FINAL DATA STORAGE")
print("==========================================")

print("Processed orders:", processed_path)
print("Overall summary:", reports_path + "/overall_summary")
print("Restaurant summary:", reports_path + "/restaurant_summary")
print("Status summary:", reports_path + "/status_summary")

print("\n==========================================")
print("CONSOLIDATED ANALYTICS COMPLETED")
print("==========================================")

spark.stop()
