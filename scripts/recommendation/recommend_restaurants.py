
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, lit, round, greatest, least,
    when, instr
)

# ==========================================
# 1. CREATE SPARK SESSION
# ==========================================

spark = (
    SparkSession.builder
    .appName("FoodieAnalytics_Recommendation_Engine")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

input_path = (
    "/mnt/c/BDA-miniproject/dataset/processed/"
    "clustered_restaurants"
)

output_path = (
    "/mnt/c/BDA-miniproject/reports/recommendations"
)

print("\n==========================================")
print(" FOODIEANALYTICS RESTAURANT RECOMMENDER")
print("==========================================\n")


# ==========================================
# 2. COLLECT USER PREFERENCES
# ==========================================

print("Enter your restaurant preferences:\n")

cuisine = input(
    "Preferred cuisine (e.g. Chinese, Italian, North Indian): "
).strip().lower()

location = input(
    "Preferred Bangalore locality (e.g. BTM, HSR, Whitefield): "
).strip().lower()

budget_input = input(
    "Maximum budget for two (e.g. 1000): "
).strip()

rating_input = input(
    "Minimum restaurant rating (e.g. 4.0): "
).strip()

delivery = input(
    "Online delivery required? (yes/no/any): "
).strip().lower()


# ==========================================
# 3. VALIDATE USER INPUT
# ==========================================

if budget_input:
    budget = float(budget_input)

    if budget <= 0:
        raise ValueError("Budget must be greater than zero.")
else:
    budget = None

if rating_input:
    min_rating = float(rating_input)

    if not 0 <= min_rating <= 5:
        raise ValueError("Rating must be between 0 and 5.")
else:
    min_rating = None

if delivery not in ["yes", "no", "any"]:
    raise ValueError("Delivery must be yes, no, or any.")


# ==========================================
# 4. LOAD CLUSTERED RESTAURANT DATA
# ==========================================

print("\nLoading restaurant dataset...")

df = spark.read.parquet(input_path)

print("Total restaurants available:", df.count())


# ==========================================
# 5. FILTER RESTAURANTS
# ==========================================

print("\nApplying user preferences...")

# Cuisine matching
if cuisine:
    df = df.filter(
        instr(lower(col("cuisines")), cuisine) > 0
    )

# Location matching
if location:
    df = df.filter(
        lower(col("location_clean")).contains(location)
    )

# Budget filtering
if budget is not None:
    df = df.filter(
        col("cost_for_two_num").isNotNull()
        & (col("cost_for_two_num") <= budget)
    )

# Minimum rating filtering
if min_rating is not None:
    df = df.filter(
        col("rating_num").isNotNull()
        & (col("rating_num") >= min_rating)
    )

# Delivery filtering
# Unknown delivery values are not treated as No.
if delivery == "yes":
    df = df.filter(col("online_order_bool") == True)

elif delivery == "no":
    df = df.filter(col("online_order_bool") == False)

# For "any", include Yes, No and Unknown.

matching_count = df.count()

print("Restaurants matching preferences:", matching_count)


# ==========================================
# 6. HANDLE NO MATCHING RESTAURANTS
# ==========================================

if matching_count == 0:

    print("\nNo restaurants match all selected preferences.")
    print("Try changing your budget, rating, cuisine,")
    print("location or delivery preference.")

else:

    # ======================================
    # 7. CALCULATE RECOMMENDATION COMPONENTS
    # ======================================

    print("\nCalculating recommendation scores...")

    # Cuisine match score
    if cuisine:
        df = df.withColumn("cuisine_score", lit(1.0))
    else:
        df = df.withColumn("cuisine_score", lit(1.0))

    # Rating score: normalize rating between 0 and 1
    df = df.withColumn(
        "rating_score",
        least(
            greatest(col("rating_num") / 5.0, lit(0.0)),
            lit(1.0)
        )
    )

    # Budget compatibility score
    if budget is not None:
        df = df.withColumn(
            "budget_score",
            greatest(
                lit(0.0),
                lit(1.0) -
                (col("cost_for_two_num") / lit(budget))
            )
        )
    else:
        df = df.withColumn("budget_score", lit(1.0))

    # Delivery preference score
    if delivery == "any":
        df = df.withColumn("delivery_score", lit(1.0))

    elif delivery == "yes":
        df = df.withColumn("delivery_score", lit(1.0))

    else:
        df = df.withColumn("delivery_score", lit(1.0))

    # Location preference score
    if location:
        df = df.withColumn("location_score", lit(1.0))
    else:
        df = df.withColumn("location_score", lit(1.0))

    # ======================================
    # 8. CALCULATE FINAL WEIGHTED SCORE
    # ======================================

    df = df.withColumn(
        "recommendation_score",
        (
            0.40 * col("cuisine_score")
            + 0.25 * col("rating_score")
            + 0.15 * col("budget_score")
            + 0.10 * col("delivery_score")
            + 0.10 * col("location_score")
        ) * 100
    )

    df = df.withColumn(
        "recommendation_score",
        round(col("recommendation_score"), 2)
    )

    # ======================================
    # 9. RANK RESTAURANTS
    # ======================================

    recommendations = df.orderBy(
        col("recommendation_score").desc(),
        col("rating_num").desc(),
        col("votes_num").desc()
    )

    print("\n==========================================")
    print(" TOP 10 RECOMMENDED RESTAURANTS")
    print("==========================================\n")

    recommendations.select(
        "name",
        "location_clean",
        "cuisines",
        "rating_num",
        "votes_num",
        "cost_for_two_num",
        "online_order_bool",
        "cluster_id",
        "recommendation_score"
    ).show(10, truncate=False)

    # ======================================
    # 10. SAVE RECOMMENDATION RESULTS
    # ======================================

    print("\nSaving recommendation results...")

    recommendations.select(
        "name",
        "location_clean",
        "cuisines",
        "rating_num",
        "votes_num",
        "cost_for_two_num",
        "online_order_bool",
        "cluster_id",
        "recommendation_score"
    ).limit(10).coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(output_path)

    print("\nRecommendations saved at:", output_path)

    print("\n==========================================")
    print("RECOMMENDATION ENGINE COMPLETED")
    print("==========================================")


# ==========================================
# 11. STOP SPARK SESSION
# ==========================================

spark.stop()
