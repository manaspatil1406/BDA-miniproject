from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, round
from pyspark.ml.clustering import KMeans

# -----------------------------------------
# 1. Create Spark Session
# -----------------------------------------

spark = (
    SparkSession.builder
    .appName("FoodieAnalytics_Restaurant_Clustering")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

input_path = "/mnt/c/BDA-miniproject/dataset/processed/feature_vectors"
output_path = "/mnt/c/BDA-miniproject/dataset/processed/clustered_restaurants"
model_path = "/mnt/c/BDA-miniproject/models/recommendation/kmeans_model"

print("\n==========================================")
print("FOODIEANALYTICS - RESTAURANT CLUSTERING")
print("==========================================\n")

# -----------------------------------------
# 2. Load Feature Vectors
# -----------------------------------------

print("Loading restaurant feature vectors...")

df = spark.read.parquet(input_path)

print("Total records:", df.count())

# Retain only the fields required for clustering and reporting
df = df.select(
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
)

# -----------------------------------------
# 3. Initialize K-Means
# -----------------------------------------

print("\nInitializing K-Means algorithm...")

kmeans = KMeans(
    featuresCol="features",
    predictionCol="cluster_id",
    k=5,
    maxIter=20,
    seed=42,
    initMode="k-means||"
)

# -----------------------------------------
# 4. Train Clustering Model
# -----------------------------------------

print("\nTraining K-Means model...")

model = kmeans.fit(df)

print("K-Means model training completed!")

# -----------------------------------------
# 5. Assign Restaurants to Clusters
# -----------------------------------------

print("\nAssigning restaurants to clusters...")

clustered_df = model.transform(df)

# -----------------------------------------
# 6. Display Cluster Assignments
# -----------------------------------------

print("\nSample restaurant cluster assignments:")

clustered_df.select(
    "name",
    "primary_cuisine",
    "rating_num",
    "cost_for_two_num",
    "cluster_id"
).show(15, truncate=False)

# -----------------------------------------
# 7. Analyze Cluster Distribution
# -----------------------------------------

print("\n==========================================")
print("RESTAURANT CLUSTER DISTRIBUTION")
print("==========================================")

clustered_df.groupBy("cluster_id") \
    .agg(count("*").alias("total_restaurants")) \
    .orderBy("cluster_id") \
    .show()

# -----------------------------------------
# 8. Generate Cluster Statistics
# -----------------------------------------

print("\n==========================================")
print("CLUSTER-WISE ANALYTICS")
print("==========================================")

cluster_summary = (
    clustered_df.groupBy("cluster_id")
    .agg(
        count("*").alias("total_restaurants"),
        round(avg("rating_num"), 2).alias("average_rating"),
        round(avg("cost_for_two_num"), 2).alias("average_cost_for_two"),
        round(avg("votes_num"), 2).alias("average_votes")
    )
    .orderBy("cluster_id")
)

cluster_summary.show(truncate=False)

# -----------------------------------------
# 9. Display Cluster Centers
# -----------------------------------------

print("\n==========================================")
print("K-MEANS CLUSTER CENTERS")
print("==========================================")

for index, center in enumerate(model.clusterCenters()):
    print(f"Cluster {index}: {center}")

# -----------------------------------------
# 10. Save Clustered Dataset
# -----------------------------------------

print("\nSaving clustered restaurant dataset...")

clustered_df.repartition(2) \
    .write.mode("overwrite") \
    .parquet(output_path)

# -----------------------------------------
# 11. Save Trained Model
# -----------------------------------------

print("\nSaving trained K-Means model...")

model.write().overwrite().save(model_path)

print("\nClustered dataset saved at:", output_path)
print("K-Means model saved at:", model_path)

spark.stop()

print("\n==========================================")
print("RESTAURANT CLUSTERING COMPLETED SUCCESSFULLY")
print("==========================================")
