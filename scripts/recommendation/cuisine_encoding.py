
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, element_at, lower, trim
from pyspark.ml.feature import StringIndexer, OneHotEncoder

# --------------------------------------------------
# 1. Create Spark Session with memory optimizations
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("FoodieAnalytics_Cuisine_Encoding")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n==========================================")
print("FOODIEANALYTICS - CUISINE FEATURE ENCODING")
print("==========================================")

# --------------------------------------------------
# 2. Define Paths
# --------------------------------------------------

input_path = (
    "/mnt/c/BDA-miniproject/"
    "dataset/processed/recommendation_ready"
)

output_path = (
    "/mnt/c/BDA-miniproject/"
    "dataset/processed/cuisine_encoded"
)

model_path = (
    "/mnt/c/BDA-miniproject/"
    "models/recommendation/cuisine_encoder"
)

# --------------------------------------------------
# 3. Load Only Required Columns
# --------------------------------------------------

print("\nLoading recommendation-ready dataset...")

df = spark.read.parquet(input_path)

# Exclude large unnecessary columns such as
# reviews_list and menu_item to reduce memory usage.

required_columns = [
    "name",
    "url",
    "rest_type",
    "location_clean",
    "cuisines",
    "cuisine_list",
    "rating_num",
    "votes_num",
    "cost_for_two_num",
    "online_order_bool",
    "book_table_bool"
]

df = df.select(*required_columns)

print("Required columns selected.")
print("Records:", df.count())

# --------------------------------------------------
# 4. Extract Primary Cuisine
# --------------------------------------------------

print("\nExtracting primary cuisine...")

df = (
    df
    .withColumn(
        "primary_cuisine",
        lower(trim(element_at(col("cuisine_list"), 1)))
    )
)

encoding_df = df.filter(
    col("primary_cuisine").isNotNull()
    & (col("primary_cuisine") != "")
)

print(
    "Records available for encoding:",
    encoding_df.count()
)

# --------------------------------------------------
# 5. Apply StringIndexer
# --------------------------------------------------

print("\nApplying StringIndexer...")

indexer = StringIndexer(
    inputCol="primary_cuisine",
    outputCol="cuisine_index",
    handleInvalid="keep"
)

indexer_model = indexer.fit(encoding_df)

indexed_df = indexer_model.transform(encoding_df)

print("\nCuisine categories and indexes:")

for index, cuisine in enumerate(indexer_model.labels):
    print(f"{cuisine} -> {index}")

print(
    "\nUnique primary cuisine categories:",
    len(indexer_model.labels)
)

# --------------------------------------------------
# 6. Apply OneHotEncoder
# --------------------------------------------------

print("\nApplying OneHotEncoder...")

encoder = OneHotEncoder(
    inputCols=["cuisine_index"],
    outputCols=["cuisine_vector"],
    handleInvalid="keep"
)

encoder_model = encoder.fit(indexed_df)

encoded_df = encoder_model.transform(indexed_df)

# --------------------------------------------------
# 7. Display Sample Results
# --------------------------------------------------

print("\nEncoded Cuisine Records:")

encoded_df.select(
    "name",
    "primary_cuisine",
    "cuisine_index",
    "cuisine_vector"
).show(10, truncate=False)

# --------------------------------------------------
# 8. Display Encoding Statistics
# --------------------------------------------------

print("\nCuisine Encoding Statistics:")

vector_size = (
    encoded_df
    .select("cuisine_vector")
    .first()["cuisine_vector"]
    .size
)

print("Unique cuisine categories:", len(indexer_model.labels))
print("One-hot feature vector size:", vector_size)

# --------------------------------------------------
# 9. Save Encoded Dataset
# --------------------------------------------------

print("\nSaving cuisine-encoded dataset...")

(
    encoded_df
    .repartition(2)
    .write
    .mode("overwrite")
    .parquet(output_path)
)

print("Encoded dataset saved successfully.")

# --------------------------------------------------
# 10. Save MLlib Models
# --------------------------------------------------

print("\nSaving encoding models...")

indexer_model.write().overwrite().save(
    model_path + "/string_indexer"
)

encoder_model.write().overwrite().save(
    model_path + "/one_hot_encoder"
)

print("StringIndexer model saved.")
print("OneHotEncoder model saved.")

# --------------------------------------------------
# 11. Stop Spark
# --------------------------------------------------

spark.stop()

print("\n==========================================")
print("CUISINE ENCODING COMPLETED SUCCESSFULLY")
print("==========================================")
