from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log1p, coalesce, lit
from pyspark.ml.feature import VectorAssembler, StandardScaler

# -----------------------------------------
# 1. Create Spark Session
# -----------------------------------------

spark = (
    SparkSession.builder
    .appName("FoodieAnalytics_Feature_Vector")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

input_path = "/mnt/c/BDA-miniproject/dataset/processed/cuisine_encoded"
output_path = "/mnt/c/BDA-miniproject/dataset/processed/feature_vectors"
scaler_path = "/mnt/c/BDA-miniproject/models/recommendation/numeric_scaler"

print("\n==========================================")
print("FOODIEANALYTICS - FEATURE VECTOR CONSTRUCTION")
print("==========================================\n")

# -----------------------------------------
# 2. Load Cuisine-Encoded Dataset
# -----------------------------------------

print("Loading cuisine-encoded restaurant data...")

df = spark.read.parquet(input_path)

required_columns = [
    "name",
    "url",
    "rest_type",
    "location_clean",
    "cuisines",
    "primary_cuisine",
    "rating_num",
    "votes_num",
    "cost_for_two_num",
    "online_order_bool",
    "book_table_bool",
    "cuisine_vector"
]

df = df.select(*required_columns)

print("Total restaurant records:", df.count())

# -----------------------------------------
# 3. Handle Missing Numerical Values
# -----------------------------------------

print("\nHandling missing numerical values...")

df = (
    df.withColumn("rating_num", col("rating_num").cast("double"))
      .withColumn("votes_num", col("votes_num").cast("double"))
      .withColumn("cost_for_two_num", col("cost_for_two_num").cast("double"))
      .withColumn(
          "online_order_num",
          col("online_order_bool").cast("double")
      )
      .withColumn(
          "book_table_num",
          col("book_table_bool").cast("double")
      )
)

# Calculate average values for missing numerical fields
averages = df.select(
    "rating_num",
    "votes_num",
    "cost_for_two_num"
).summary("mean").collect()

mean_values = {
    row["summary"]: row.asDict()
    for row in averages
}

rating_mean = float(mean_values["mean"]["rating_num"])
votes_mean = float(mean_values["mean"]["votes_num"])
cost_mean = float(mean_values["mean"]["cost_for_two_num"])

df = df.fillna({
    "rating_num": rating_mean,
    "votes_num": votes_mean,
    "cost_for_two_num": cost_mean,
    "online_order_num": 0.0,
    "book_table_num": 0.0
})

# -----------------------------------------
# 4. Transform Numerical Features
# -----------------------------------------

print("\nTransforming numerical features...")

# Log transformation reduces the effect of very large vote counts
df = df.withColumn("votes_log", log1p(col("votes_num")))

numeric_columns = [
    "rating_num",
    "votes_log",
    "cost_for_two_num",
    "online_order_num",
    "book_table_num"
]

# -----------------------------------------
# 5. Assemble Numerical Features
# -----------------------------------------

print("\nAssembling numerical features...")

numeric_assembler = VectorAssembler(
    inputCols=numeric_columns,
    outputCol="numeric_features",
    handleInvalid="error"
)

numeric_df = numeric_assembler.transform(df)

# -----------------------------------------
# 6. Scale Numerical Features
# -----------------------------------------

print("\nScaling numerical features...")

scaler = StandardScaler(
    inputCol="numeric_features",
    outputCol="scaled_numeric_features",
    withMean=True,
    withStd=True
)

scaler_model = scaler.fit(numeric_df)
scaled_df = scaler_model.transform(numeric_df)

# -----------------------------------------
# 7. Construct Final Feature Vector
# -----------------------------------------

print("\nConstructing final restaurant feature vectors...")

final_assembler = VectorAssembler(
    inputCols=["cuisine_vector", "scaled_numeric_features"],
    outputCol="features",
    handleInvalid="error"
)

final_df = final_assembler.transform(scaled_df)

# -----------------------------------------
# 8. Display Sample Results
# -----------------------------------------

print("\nSample feature vectors:")

final_df.select(
    "name",
    "primary_cuisine",
    "rating_num",
    "votes_num",
    "cost_for_two_num",
    "features"
).show(10, truncate=False)

# -----------------------------------------
# 9. Display Feature Information
# -----------------------------------------

feature_size = final_df.select("features").first()["features"].size

print("\n==========================================")
print("FEATURE VECTOR SUMMARY")
print("==========================================")

print("Total restaurant records:", final_df.count())
print("Cuisine feature dimensions:",
      final_df.select("cuisine_vector").first()["cuisine_vector"].size)
print("Numerical feature dimensions:", len(numeric_columns))
print("Total feature vector dimensions:", feature_size)

# -----------------------------------------
# 10. Save Feature Vectors
# -----------------------------------------

print("\nSaving feature vectors...")

final_df.select(
    "name",
    "url",
    "rest_type",
    "location_clean",
    "cuisines",
    "primary_cuisine",
    "rating_num",
    "votes_num",
    "cost_for_two_num",
    "online_order_bool",
    "book_table_bool",
    "features"
).repartition(2).write.mode("overwrite").parquet(output_path)

# Save the numerical scaler model
scaler_model.write().overwrite().save(scaler_path)

print("\nFeature vectors saved successfully!")
print("Output:", output_path)
print("Scaler model:", scaler_path)

spark.stop()

print("\n==========================================")
print("FEATURE VECTOR CONSTRUCTION COMPLETED")
print("==========================================")
