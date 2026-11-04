
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
    round
)

# ==========================================
# FOODIEANALYTICS - STREAMING ANALYTICS
# ==========================================

spark = (
    SparkSession.builder
    .appName("FoodieAnalytics_Streaming_Analytics")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 1. DEFINE PATHS
# ==========================================

input_path = (
    "/mnt/c/BDA-miniproject/dataset/streaming/orders_input"
)

checkpoint_path = (
    "/mnt/c/BDA-miniproject/dataset/streaming/analytics_checkpoint"
)

print("\n==========================================")
print(" FOODIEANALYTICS - STREAMING ANALYTICS")
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
# 3. READ JSON FILES AS STREAM
# ==========================================

print("\nInitializing streaming source...")

orders_stream = (
    spark.readStream
    .format("json")
    .schema(order_schema)
    .option("maxFilesPerTrigger", 1)
    .load(input_path)
)

# ==========================================
# 4. PROCESS EACH MICRO-BATCH
# ==========================================

def process_batch(batch_df, batch_id):

    print("\n")
    print("==========================================")
    print(f" PROCESSING MICRO-BATCH: {batch_id}")
    print("==========================================")

    batch_df.cache()

    total_orders = batch_df.count()

    if total_orders == 0:
        print("No orders found in this batch.")
        batch_df.unpersist()
        return

    # --------------------------------------
    # A. Overall Order Statistics
    # --------------------------------------

    print("\n--- OVERALL ORDER STATISTICS ---")

    batch_df.agg(
        count("*").alias("total_orders"),
        round(sum("order_amount"), 2).alias("total_revenue"),
        round(avg("order_amount"), 2).alias("average_order_value")
    ).show(truncate=False)

    # --------------------------------------
    # B. Restaurant-wise Order Statistics
    # --------------------------------------

    print("\n--- RESTAURANT-WISE ORDER STATISTICS ---")

    batch_df.groupBy("restaurant_id").agg(
        count("*").alias("total_orders"),
        round(sum("order_amount"), 2).alias("total_revenue")
    ).orderBy("restaurant_id").show(truncate=False)

    # --------------------------------------
    # C. Order Status Statistics
    # --------------------------------------

    print("\n--- ORDER STATUS STATISTICS ---")

    batch_df.groupBy("order_status").agg(
        count("*").alias("total_orders")
    ).orderBy("order_status").show(truncate=False)

    print(f"Micro-batch {batch_id} processed successfully.")

    batch_df.unpersist()


# ==========================================
# 5. START STREAMING ANALYTICS
# ==========================================

print("\nStarting streaming analytics...")
print("Processing available JSON files...\n")

query = (
    orders_stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

# ==========================================
# 6. WAIT FOR COMPLETION
# ==========================================

query.awaitTermination()

print("\n==========================================")
print("STREAMING ANALYTICS COMPLETED")
print("==========================================")

print("All available order batches have been processed.")

spark.stop()
