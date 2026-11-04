
from pyspark.sql import SparkSession

# Create SparkSession
spark = (
    SparkSession.builder
    .appName("FoodieAnalytics")
    .master("local[*]")
    .getOrCreate()
)

print("\n===================================")
print(" FOODIE ANALYTICS - SPARK TEST")
print("===================================")

# Display Spark version
print("Spark Version:", spark.version)

# Create sample data
data = [
    (1, "Restaurant A", "Chinese", 4.5),
    (2, "Restaurant B", "Indian", 4.2),
    (3, "Restaurant C", "Italian", 4.8),
    (4, "Restaurant D", "Mexican", 3.9)
]

columns = ["id", "restaurant_name", "cuisine", "rating"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

print("\nRestaurant Data:")
df.show()

# Perform an analytical operation
print("\nRestaurants with rating >= 4.5:")

df.filter(df.rating >= 4.5).show()

# Stop SparkSession
spark.stop()

print("===================================")
print(" SPARK TEST COMPLETED SUCCESSFULLY")
print("===================================")
