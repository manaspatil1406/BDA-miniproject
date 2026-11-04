
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

# ==========================================
# FOODIEANALYTICS - SPARK STREAMING CONSUMER
# ==========================================

spark = (
    SparkSession.builder
    .appName("FoodieAnalytics_Streaming_Consumer")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 1. DEFINE INPUT AND CHECKPOINT PATHS
# ==========================================

input_path = (
    "/mnt/c/BDA-miniproject/dataset/streaming/orders_input"
)

checkpoint_path = (
    "/mnt/c/BDA-miniproject/dataset/streaming/checkpoint"
)

print("\n==========================================")
print(" FOODIEANALYTICS - SPARK STREAMING")
print("==========================================")

print("Input directory:", input_path)
print("Checkpoint directory:", checkpoint_path)

# ==========================================
# 2. DEFINE ORDER DATA SCHEMA
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
# 3. READ JSON FILES AS A STREAM
# ==========================================

print("\nInitializing JSON streaming source...")

orders_stream = (
    spark.readStream
    .format("json")
    .schema(order_schema)
    .option("maxFilesPerTrigger", 1)
    .load(input_path)
)

# ==========================================
# 4. DISPLAY INCOMING ORDERS
# ==========================================

print("\nStarting streaming query...")
print("Waiting for incoming order batches...\n")

query = (
    orders_stream
    .select(
        "order_id",
        "restaurant_id",
        "customer_id",
        "order_amount",
        "order_status",
        "order_timestamp"
    )
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("numRows", 20)
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime="5 seconds")
    .start()
)

# ==========================================
# 5. KEEP STREAMING APPLICATION RUNNING
# ==========================================

try:
    query.awaitTermination()

except KeyboardInterrupt:
    print("\nStopping streaming application...")

finally:
    if query.isActive:
        query.stop()

    spark.stop()

    print("\nStreaming application stopped.")
