
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, sum as spark_sum

# Create SparkSession
spark = (
    SparkSession.builder
    .appName("FoodieAnalytics_DataLoading")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Dataset path
DATASET_PATH = (
    "/mnt/c/BDA-miniproject/"
    "dataset/processed/hadoop_safe_zomato.csv"
)

print("\n======================================")
print(" FOODIE ANALYTICS - DATASET LOADING")
print("======================================")

# Load CSV into Spark DataFrame
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .option("multiLine", "false")
    .option("quote", '"')
    .option("escape", '"')
    .option("mode", "PERMISSIVE")
    .option("maxCharsPerColumn", "10000000")
    .csv(DATASET_PATH)
)

# Display schema
print("\n1. DATAFRAME SCHEMA")
df.printSchema()

# Display column names
print("\n2. COLUMN INFORMATION")
print("Total columns:", len(df.columns))
print(df.columns)

# Verify record count
print("\n3. TOTAL RECORD COUNT")
total_records = df.count()
print("Total records:", total_records)

# Display selected columns only
print("\n4. FIRST 10 RESTAURANT RECORDS")
df.select(
    "name",
    "location",
    "rate",
    "votes",
    "cuisines"
).show(10, truncate=50)

# Missing-value analysis
print("\n5. MISSING VALUE ANALYSIS")

missing_expressions = [
    spark_sum(
        when(
            col(column_name).isNull() |
            (trim(col(column_name)) == ""),
            1
        ).otherwise(0)
    ).alias(column_name)
    for column_name in df.columns
]

missing_values = df.agg(*missing_expressions)
missing_values.show(truncate=False)

print("\n======================================")
print(" DATASET LOADING COMPLETED")
print("======================================")

spark.stop()
