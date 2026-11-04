from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower

spark = (
    SparkSession.builder
    .appName("FoodieAnalytics_Filter_Diagnosis")
    .master("local[2]")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

path = "/mnt/c/BDA-miniproject/dataset/processed/clustered_restaurants"

df = spark.read.parquet(path)

print("\n========== FILTER DIAGNOSTICS ==========")

print("1. Total restaurants:", df.count())

# Cuisine filter
cuisine_df = df.filter(
    lower(col("cuisines")).contains("chinese")
)
print("2. Chinese restaurants:", cuisine_df.count())

# Location filter
location_df = df.filter(
    lower(col("location_clean")).contains("btm")
)
print("3. Restaurants in BTM:", location_df.count())

# Cuisine + Location
both_df = cuisine_df.filter(
    lower(col("location_clean")).contains("btm")
)
print("4. Chinese restaurants in BTM:", both_df.count())

# Add budget
budget_df = both_df.filter(
    col("cost_for_two_num") <= 1000
)
print("5. Chinese in BTM within budget:", budget_df.count())

# Add rating
rating_df = budget_df.filter(
    col("rating_num") >= 4.0
)
print("6. Chinese in BTM within budget and rating:", rating_df.count())

# Add delivery
delivery_df = rating_df.filter(
    col("online_order_bool") == True
)
print("7. Matching all preferences:", delivery_df.count())

print("\n========== SAMPLE BTM CHINESE RESTAURANTS ==========")

both_df.select(
    "name",
    "location_clean",
    "cuisines",
    "rating_num",
    "cost_for_two_num",
    "online_order_bool"
).show(20, truncate=False)

spark.stop()
