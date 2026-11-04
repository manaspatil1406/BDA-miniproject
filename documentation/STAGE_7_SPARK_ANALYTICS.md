# Stage 7 – Apache Spark and Spark SQL Analytics: Comprehensive Report

**Project:** FoodieAnalytics: Food Delivery Analytics and Restaurant Recommendation System using Big Data  
**Stage:** 7 – Distributed In-Memory Processing & Analytical Querying with Apache Spark  
**Dataset:** Zomato Bangalore Restaurant Dataset (51,656 records, 17 features)  
**Execution Environment:** Ubuntu 24.04.5 LTS on WSL2, Python 3.12.3, Apache Spark (PySpark) 4.0.1, Apache Hadoop 3.4.3, OpenJDK 21  

---

## A. Introduction

### 1. Purpose of Apache Spark in FoodieAnalytics
Apache Spark is a unified, distributed, general-purpose cluster-computing framework engineered for high-speed data processing at scale. In the **FoodieAnalytics** pipeline, Apache Spark serves as the high-performance in-memory analytical engine. While earlier stages established distributed storage on Hadoop HDFS (Stage 3), batch MapReduce computation (Stage 4), and data warehousing with Hive (Stage 5), Stage 7 leverages Spark to perform comprehensive multi-dimensional aggregations, statistical evaluations, and complex relational queries on restaurant data.

### 2. Why Spark Was Introduced After Hadoop and Hive
Although Hadoop MapReduce and Apache Hive provided scalable distributed processing, modern big data workflows transition to Apache Spark for distinct architectural advantages:

| Dimension | Hadoop MapReduce / Hive (Stages 4 & 5) | Apache Spark & Spark SQL (Stage 7) |
| :--- | :--- | :--- |
| **Execution Model** | Disk-bound two-stage execution (Map $\rightarrow$ Disk $\rightarrow$ Shuffle $\rightarrow$ Reduce $\rightarrow$ Disk). | In-memory Directed Acyclic Graph (DAG) execution with pipelined transformations. |
| **Performance** | High disk I/O and serialization overhead per job (queries take 1–2 minutes). | Up to 10–100× faster execution via memory caching and Catalyst query optimizer. |
| **API & Expressiveness** | Low-level key-value mapper/reducer code or declarative HiveQL. | Unified DataFrame API supporting programmatic transformations alongside ANSI SQL. |
| **Data Types & Parsing** | Strict text/string parsing; SerDe constraints in Hive. | Rich typed schema, regex extraction, UDFs, and complex collection functions. |
| **Ecosystem Synergy** | Standalone batch or warehouse tools. | Seamless foundation for future Machine Learning (MLlib) and Graph analytics. |

### 3. Role of Distributed Data Processing
In food delivery platforms such as Zomato or Swiggy, catalogs contain millions of dynamic menu listings, real-time reviews, and customer orders. Single-node processing libraries (e.g., standard pandas) are constrained by host RAM and single-threaded operations. Distributed processing partitions the dataset across cluster workers, executes concurrent transformations, and handles schema irregularities without memory exhaustion.

---

## B. Environment and Technologies

The Stage 7 implementation was executed in a native Linux subsystem environment with the following verified software stack:

- **Operating System:** Ubuntu 24.04.5 LTS on Windows Subsystem for Linux (WSL2)
- **Python Runtime:** Python 3.12.3 (isolated virtual environment)
- **Java Virtual Machine:** OpenJDK 64-Bit Server VM (build 21.0.6+7-Ubuntu-124.04.1)
- **Distributed Computing Engine:** Apache Spark / PySpark 4.0.1
- **Underlying Storage Framework:** Apache Hadoop 3.4.3 (HDFS integration)
- **Query Engine:** Spark SQL (Catalyst Optimizer & Tungsten Execution Engine)
- **Data Serialization:** Apache Arrow & Standard CSV Parser (`PERMISSIVE` mode)

---

## C. Implementation Steps

The Stage 7 analytics pipeline was completed through a structured, multi-step engineering workflow:

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Environment & Session Verification (test_spark.py)       │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Robust CSV Ingestion (load_restaurant_data.py)           │
│    - dataset/processed/hadoop_safe_zomato.csv               │
│    - maxCharsPerColumn=10000000, header=true                │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. DataFrame Type Transformations                           │
│    (dataframe_transformations.py)                           │
│    - Regex cleaning: rating_num, votes_num, cost_for_two_num│
│    - Boolean casting: online_order_bool, book_table_bool    │
│    - Feature engineering: rating_category, price_category   │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Temporary SQL View Registration                          │
│    - df.createOrReplaceTempView("restaurants")              │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Spark SQL Query Execution (spark_sql_analytics.py)       │
│    - 8 analytical SQL scripts executed                      │
│    - Dedicated .sql scripts in scripts/spark/queries/       │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. Partitioned CSV Export (reports/spark_results/)          │
│    - Overwrite mode with CSV part files & _SUCCESS markers  │
└─────────────────────────────────────────────────────────────┘
```

1. **PySpark Environment Setup:** Configured Python virtual environment on Ubuntu WSL2, setting `JAVA_HOME`, `HADOOP_HOME`, and `SPARK_HOME` paths.
2. **SparkSession Creation & Verification:** Initialized `SparkSession` with application name `"FoodieAnalytics"` and `local[*]` master to utilize all available CPU cores. Verified via [`scripts/spark/test_spark.py`](../../scripts/spark/test_spark.py).
3. **Loading the Hadoop-Safe Zomato Dataset:** Ingested `dataset/processed/hadoop_safe_zomato.csv` using `spark.read` with explicit delimiter and quoting options via [`scripts/spark/load_restaurant_data.py`](../../scripts/spark/load_restaurant_data.py).
4. **Creating and Inspecting the Spark DataFrame:** Audited schema structure, verified row count (51,656), and analyzed column missingness using Spark aggregations.
5. **Data Type Conversion and Derived Columns:** Formatted raw strings into strongly typed columns and engineered descriptive categories using `pyspark.sql.functions` in [`scripts/spark/dataframe_transformations.py`](../../scripts/spark/dataframe_transformations.py).
6. **DataFrame Transformations and Aggregations:** Evaluated programmatic filtering, group-by summaries, multi-column statistics, and data quality metrics.
7. **Registering Temporary SQL View:** Registered the preprocessed DataFrame as an in-memory SQL view named `restaurants` using `createOrReplaceTempView("restaurants")`.
8. **Executing Spark SQL Queries:** Executed eight analytical SQL queries in [`scripts/spark/spark_sql_analytics.py`](../../scripts/spark/spark_sql_analytics.py), covering geographic distributions, culinary trends, cost tiers, and rating dynamics.
9. **Saving Query Results as CSV Outputs:** Exported all query result sets into designated subdirectories in [`reports/spark_results/`](../../reports/spark_results/) with header rows and `_SUCCESS` verification markers.

---

## D. Dataset Details

- **Total Ingested Records:** **51,656** restaurants
- **Total Ingested Columns:** **17** attributes
- **Source File Used:** [`dataset/processed/hadoop_safe_zomato.csv`](../../dataset/processed/hadoop_safe_zomato.csv)
- **Preservation of Original Data:** The original raw dataset ([`dataset/raw/zomato.csv`](../../dataset/raw/zomato.csv)) and the cleaned baseline ([`dataset/processed/cleaned_zomato.csv`](../../dataset/processed/cleaned_zomato.csv)) were preserved strictly intact.

### Why `hadoop_safe_zomato.csv` Was Used
During Stage 2 data cleaning, the cleaned dataset retained customer review tuples in the `reviews_list` column. Because customer reviews contain embedded newline characters within quotes, the physical line count of `cleaned_zomato.csv` expanded to **74,669 physical lines** for **51,657 logical records** (~23,012 embedded newlines).

Distributed split-based CSV readers (such as Hadoop TextInputFormat and Spark's default parallel line reader) split data blocks by byte offset and newline boundaries. Unescaped multiline fields can cause record tearing, schema shifts, or data truncation across partitions. The newline-safe dataset (`hadoop_safe_zomato.csv`) replaced internal review newlines with spaces, guaranteeing a strict 1:1 correspondence between physical lines and logical records (51,657 lines including header).

---

## E. DataFrame Transformations & Derived Columns

In [`scripts/spark/dataframe_transformations.py`](../../scripts/spark/dataframe_transformations.py) and [`scripts/spark/spark_sql_analytics.py`](../../scripts/spark/spark_sql_analytics.py), the raw string DataFrame was enriched with typed and categorized columns:

### 1. Feature Transformation Definitions

| Derived Column | Target Data Type | Transformation Logic |
| :--- | :---: | :--- |
| `rating_num` | `DoubleType` | Regex extraction `r"^([0-5](?:\.[0-9]+)?)"` on `rate`, trimmed and cast to double. Values constrained to $[1.0, 5.0]$. Missing/unrated (`'NEW'`, `'-'`) evaluate to `NULL`. |
| `votes_num` | `LongType` | Removed comma thousands-separators via `regexp_replace(col("votes"), ",", "")` and cast to `long`. |
| `cost_for_two_num`| `DoubleType` | Stripped commas from `approx_cost_for_two_people`, extracted numeric patterns `r"([0-9]+(?:\.[0-9]+)?)"`, and cast to `double`. |
| `online_order_bool`| `BooleanType`| Case-insensitive evaluation: `LOWER(TRIM(online_order)) == "true"` $\rightarrow$ `True`, `"false"` $\rightarrow$ `False`, else `NULL`. |
| `book_table_bool` | `BooleanType`| Case-insensitive evaluation: `LOWER(TRIM(book_table)) == "true"` $\rightarrow$ `True`, `"false"` $\rightarrow$ `False`, else `NULL`. |
| `rating_category` | `StringType` | Conditional tier assignment: $\ge 4.5 \rightarrow \text{Excellent}$, $\ge 4.0 \rightarrow \text{Very Good}$, $\ge 3.0 \rightarrow \text{Good}$, $\ge 2.0 \rightarrow \text{Average}$, $< 2.0 \rightarrow \text{Below Average}$, `NULL` $\rightarrow \text{Not Rated}$. |
| `price_category` | `StringType` | Economic tier segmentation based on `cost_for_two_num`: $\le 500 \rightarrow \text{Budget}$, $\le 1000 \rightarrow \text{Moderate}$, $\le 2000 \rightarrow \text{Expensive}$, $> 2000 \rightarrow \text{Premium}$, `NULL` $\rightarrow \text{Unknown}$. |

### 2. Category Distributions & Quality Metrics

#### Price Category Distribution (Actual Execution Output)
From `query07_price_range_distribution`:

| Price Category | Cost Range (INR) | Restaurant Count | Percentage |
| :--- | :---: | :---: | :---: |
| **Budget** | $\le ₹500$ | **33,255** | 64.38% |
| **Moderate** | $₹501 - ₹1,000$ | **12,803** | 24.79% |
| **Expensive** | $₹1,001 - ₹2,000$ | **4,650** | 9.00% |
| **Premium** | $> ₹2,000$ | **624** | 1.21% |
| **Unknown** | Missing / Null | **324** | 0.63% |
| **Total** | — | **51,656** | **100.00%** |

#### Data Quality Summary
- **Total Records:** 51,656
- **Valid Ratings (`rating_num`):** 41,640 (80.61% rated; 10,016 unrated/missing)
- **Valid Votes (`votes_num`):** 51,656 (100.00% complete)
- **Valid Cost Values (`cost_for_two_num`):** 51,332 (99.37% valid; 324 nulls)

---

## F. Spark SQL Queries

All eight SQL queries were executed against the temporary view `restaurants` and exported to CSV directories.

```
scripts/spark/queries/
├── query01_restaurants_by_location.sql
├── query02_popular_cuisines.sql
├── query03_average_rating_by_location.sql
├── query04_top_rated_restaurants.sql
├── query05_average_cost_by_location.sql
├── query06_online_delivery_analysis.sql
├── query07_price_range_distribution.sql
└── query08_rating_votes_relationship.sql
```

### Query Summary Table

| Query No. | Analytical Operation | SQL Script | Output Directory | Output Records |
| :---: | :--- | :--- | :--- | :---: |
| **1** | Restaurants by Locality | [`query01_restaurants_by_location.sql`](../../scripts/spark/queries/query01_restaurants_by_location.sql) | [`reports/spark_results/query01_restaurants_by_location/`](../../reports/spark_results/query01_restaurants_by_location/) | 93 localities |
| **2** | Popular Cuisines (Top 20) | [`query02_popular_cuisines.sql`](../../scripts/spark/queries/query02_popular_cuisines.sql) | [`reports/spark_results/query02_popular_cuisines/`](../../reports/spark_results/query02_popular_cuisines/) | 20 cuisines |
| **3** | Average Rating by Locality | [`query03_average_rating_by_location.sql`](../../scripts/spark/queries/query03_average_rating_by_location.sql) | [`reports/spark_results/query03_average_rating_by_location/`](../../reports/spark_results/query03_average_rating_by_location/) | 92 localities |
| **4** | Top-Rated Restaurants (Top 10) | [`query04_top_rated_restaurants.sql`](../../scripts/spark/queries/query04_top_rated_restaurants.sql) | [`reports/spark_results/query04_top_rated_restaurants/`](../../reports/spark_results/query04_top_rated_restaurants/) | 10 restaurants |
| **5** | Average Cost for Two by Locality | [`query05_average_cost_by_location.sql`](../../scripts/spark/queries/query05_average_cost_by_location.sql) | [`reports/spark_results/query05_average_cost_by_location/`](../../reports/spark_results/query05_average_cost_by_location/) | 93 localities |
| **6** | Online Delivery Adoption | [`query06_online_delivery_analysis.sql`](../../scripts/spark/queries/query06_online_delivery_analysis.sql) | [`reports/spark_results/query06_online_delivery_analysis/`](../../reports/spark_results/query06_online_delivery_analysis/) | 93 localities |
| **7** | Price Range Distribution | [`query07_price_range_distribution.sql`](../../scripts/spark/queries/query07_price_range_distribution.sql) | [`reports/spark_results/query07_price_range_distribution/`](../../reports/spark_results/query07_price_range_distribution/) | 5 categories |
| **8** | Rating vs. Votes Correlation | [`query08_rating_votes_relationship.sql`](../../scripts/spark/queries/query08_rating_votes_relationship.sql) | [`reports/spark_results/query08_rating_votes_relationship/`](../../reports/spark_results/query08_rating_votes_relationship/) | 1 summary row |

---

### Detailed Query Breakdown

#### Query 1: Restaurant Count by Locality
- **File:** `scripts/spark/queries/query01_restaurants_by_location.sql`
- **Objective:** Quantify restaurant concentration and identify commercial food hubs across Bangalore.
- **SQL Logic:** Grouping by `location`, filtering null/blank strings, and sorting descending by count.
- **Key Findings:** 93 localities identified. **BTM** possesses Bangalore's highest restaurant density with **5,114 restaurants** (>2× the second-highest). Other top clusters include HSR (2,522), Koramangala 5th Block (2,504), JP Nagar (2,234), Whitefield (2,142), and Indiranagar (2,082). Lowest densities: Peenya (1), Rajarajeshwari Nagar (2), Jakkur (3).
- **Output:** `reports/spark_results/query01_restaurants_by_location/`

#### Query 2: Most Popular Cuisines (Top 20)
- **File:** `scripts/spark/queries/query02_popular_cuisines.sql`
- **Objective:** Identify the most prevalent culinary offerings across all restaurant menus.
- **SQL Logic:** Unnesting comma-delimited strings via `LATERAL VIEW EXPLODE(SPLIT(cuisines, ','))`, trimming whitespace, grouping, and ordering by frequency descending with `LIMIT 20`.
- **Key Findings:** **North Indian** is the most frequently listed cuisine with **21,069** listings, followed by **Chinese** (**15,534**). These two surpass local **South Indian** (**8,638**) and **Fast Food** (**8,087**), reflecting Bangalore's diverse, pan-Indian demographic. Top 10 also features Biryani (6,483), Continental (5,759), Desserts (5,631), Cafe (5,300), Beverages (4,743), and Italian (3,386).
- **Output:** `reports/spark_results/query02_popular_cuisines/`

#### Query 3: Average Restaurant Rating by Locality
- **File:** `scripts/spark/queries/query03_average_rating_by_location.sql`
- **Objective:** Gauge neighborhood culinary reputation and customer satisfaction.
- **SQL Logic:** Filtering valid ratings ($1 \le \text{rate} \le 5$), calculating `ROUND(AVG(rating_num), 2)` per locality, and counting rated establishments.
- **Key Findings:** 92 localities contain rated establishments. **Lavelle Road** leads with the highest displayed average rating of **4.14** (across 485 rated venues). Other top areas: Koramangala 3rd Block (4.02; 191 venues), St. Marks Road (4.02; 343 venues), and Koramangala 5th Block (4.01; 2,319 venues). Lowest ratings: Old Madras Road (3.18; 22 venues), Bommanahalli (3.19; 144 venues), and Peenya (3.20; 1 venue).
- **Output:** `reports/spark_results/query03_average_rating_by_location/`

#### Query 4: Top-Rated Restaurants
- **File:** `scripts/spark/queries/query04_top_rated_restaurants.sql`
- **Objective:** Identify premier dining destinations based on top rating and customer engagement.
- **SQL Logic:** Grouping by `name` and `location`, computing average rating, taking `MAX(votes_num)`, and ordering by rating descending then votes descending (`LIMIT 10`).
- **Key Findings:** The maximum average rating observed is **4.9 / 5.0**. **Byg Brewski Brewing Company** (Sarjapur Road) leads the city with a **4.9** rating and **16,832 votes**. Other elite establishments include AB's - Absolute Barbecues (BTM; 4.9, 6,490 votes), Flechazo (Whitefield; 4.9, 3,004 votes), Asia Kitchen By Mainland China (Koramangala 5th Block; 4.9, 2,256 votes), and Milano Ice Cream (Indiranagar; 4.9, 2,090 votes).
- **Output:** `reports/spark_results/query04_top_rated_restaurants/`

#### Query 5: Average Cost for Two by Locality
- **File:** `scripts/spark/queries/query05_average_cost_by_location.sql`
- **Objective:** Evaluate geographic dining affordability and consumer price levels.
- **SQL Logic:** Filtering `cost_for_two_num > 0`, calculating `ROUND(AVG(cost_for_two_num), 2)` per locality, ordered descending.
- **Key Findings:** 93 localities analyzed. **Sankey Road** exhibits the highest displayed average cost for two at **₹2,505.56** (27 venues), followed by Race Course Road (₹1,309.35; 139 venues), Lavelle Road (₹1,307.97; 521 venues), and MG Road (₹1,155.70; 894 venues). The most economical areas include Peenya (₹300.00; 1 venue), City Market (₹302.37; 125 venues), Yelahanka (₹310.00; 5 venues), CV Raman Nagar (₹311.24; 89 venues), and Ejipura (₹320.51; 434 venues).
- **Output:** `reports/spark_results/query05_average_cost_by_location/`

#### Query 6: Online Delivery Adoption by Locality
- **File:** `scripts/spark/queries/query06_online_delivery_analysis.sql`
- **Objective:** Measure digital delivery platform penetration across Bangalore localities.
- **SQL Logic:** Summing conditional flags `online_order_bool = TRUE` and calculating percentage of total restaurants per locality.
- **Key Findings:** Localities with highest delivery percentages include Rajarajeshwari Nagar (100.00%; 2/2), Kanakapura Road (89.47%; 17/19), Kengeri (87.50%; 7/8), Nagawara (86.63%; 162/187), and Kumaraswamy Layout (84.62%; 165/195). High-volume commercial hubs also show strong adoption: HSR Layout (76.13%; 1,920/2,522) and BTM (65.02%; 3,325/5,114). Lowest adoption: Peenya (0.00%; 0/1), Sankey Road (0.00%; 0/27), Jakkur (0.00%; 0/3), and Majestic (13.55%; 21/155).
- **Output:** `reports/spark_results/query06_online_delivery_analysis/`

#### Query 7: Price Range Distribution
- **File:** `scripts/spark/queries/query07_price_range_distribution.sql`
- **Objective:** Profile market structure across budget, moderate, expensive, and premium price bands.
- **SQL Logic:** Grouping by `price_category` and ordering descending by restaurant count.
- **Key Findings:** Bangalore's dining market is predominantly budget-oriented: **33,255 restaurants (64.38%)** cater to budget spending ($\le ₹500$), and **12,803 (24.79%)** are moderate ($₹501–₹1000$). Combined, **89.17%** of establishments charge ₹1,000 or less for two people. Expensive dining represents 9.00% (4,650) and Premium dining accounts for just 1.21% (624).
- **Output:** `reports/spark_results/query07_price_range_distribution/`

#### Query 8: Correlation Between Ratings and Votes
- **File:** `scripts/spark/queries/query08_rating_votes_relationship.sql`
- **Objective:** Statistically evaluate the association between star ratings and customer vote counts.
- **SQL Logic:** Applying Spark SQL's built-in `CORR(rating_num, CAST(votes_num AS DOUBLE))` over all 41,640 records with valid ratings and votes.
- **Key Findings:** Pearson correlation coefficient $r = \mathbf{0.4340}$ (approximately **0.434**). This indicates a **moderate positive linear relationship**. Highly rated restaurants generally accumulate higher vote counts, driven by popularity, patronage, and word-of-mouth.
- **Output:** `reports/spark_results/query08_rating_votes_relationship/`

---

## G. Important Findings and Analytical Insights

### Summary of Key Metrics

```
========================================================================
FOODIEANALYTICS - SPARK STAGE 7 VERIFIED METRICS
========================================================================
Total Ingested Records            : 51,656
Valid Rating Records              : 41,640 (80.61%)
Valid Vote Records                : 51,656 (100.00%)
Valid Cost Records                : 51,332 (99.37%)
Highest Density Locality          : BTM (5,114 restaurants)
Top Cuisine Listed                : North Indian (21,069 listings)
Second Cuisine Listed             : Chinese (15,534 listings)
Highest Avg Rating Locality       : Lavelle Road (~4.14 / 5.0)
Most Voted Top Restaurant         : Byg Brewski Brewing Co. (4.9 rating, 16,832 votes)
Highest Avg Cost Locality         : Sankey Road (~₹2,505.56 for two)
Budget Tier Dominance             : 33,255 restaurants (64.38% <= ₹500)
Rating-Votes Correlation          : Pearson r ≈ 0.434 (moderate positive)
========================================================================
```

### Statistical & Contextual Caveats

> [!IMPORTANT]
> **Correlation Does Not Imply Causation:**  
> The Pearson correlation coefficient of $r \approx 0.434$ demonstrates a moderate statistical co-movement between ratings and votes, but it does **not** establish a causal mechanism. A high rating does not automatically cause a restaurant to receive more votes. Rather, latent confounding factors—such as physical seating capacity, operational longevity, marketing budget, brand recognition, and platform listing rank—simultaneously drive both higher footfall (more votes) and sustained quality investments (higher ratings).

> [!WARNING]
> **Small Sample Sizes in Locality Metrics:**  
> Several geographic metrics are skewed by small sample sizes:
> - In `query06` (online delivery adoption), **Rajarajeshwari Nagar** shows $100.00\%$ adoption based on only **2** total restaurants, and **Kengeri** shows $87.50\%$ based on **8** restaurants.
> - Conversely, **Peenya** ($0.00\%$ adoption, ₹300 cost, 3.2 rating) consists of only **1** restaurant listing.
> - **Sankey Road** exhibits Bangalore's highest average cost (₹2,505.56) and $0.00\%$ online ordering based on **27** venues, which are predominantly upscale clubs and luxury hotel dining rooms.
> 
> Locality rankings should always be interpreted alongside their underlying record counts (`total_restaurants` and `rated_restaurant_count`).

---

## H. Challenges and Technical Observations

During Stage 7 development, several technical constraints were navigated:

1. **Large CSV Fields & String Lengths:**
   Customer review columns (`reviews_list`) contain serialized lists of tuples with lengthy reviewer text. Standard CSV readers encounter buffer overflow errors. This was resolved by configuring `.option("maxCharsPerColumn", "10000000")` during DataFrame loading.

2. **Multiline Content & Newline Normalization:**
   Raw web scrapes frequently contain unescaped line breaks. While Stage 2 performed text cleaning, Stage 4's `prepare_hadoop_safe_csv.py` established single-line integrity. In Stage 7, verifying that Spark used `hadoop_safe_zomato.csv` ensured seamless, partition-safe parallel ingestion without split corruption.

3. **Missing Values & Type Inconsistencies:**
   The `rate` column included non-numeric strings (`'NEW'`, `'-'`), while `votes` and `approx_cost_for_two_people` contained commas. Naive type casting to `double` in Spark produces runtime exceptions or unintended zeroes. Robust regular expressions (`regexp_extract`, `regexp_replace`) were applied prior to casting, ensuring unrated entries cleanly evaluated to SQL `NULL` and avoided biasing averages.

4. **Character-Encoding Artifacts in Restaurant Names:**
   Certain restaurant names contain multi-byte UTF-8 encoding corruption from earlier web scraping. For instance, in `query04` output (rank 8), `Santé Spa Cuisine` appears as `SantAAA...c Spa Cuisine`. These artifacts exist in the source dataset and were preserved accurately without falsification.

5. **WSL2 / Hadoop Native Library Warnings:**
   During Spark execution, benign console warnings appeared:
   `WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable`.
   These warnings are standard in WSL2 development environments lacking custom-compiled GLIBC native binaries and have no impact on execution correctness or analytical precision.

---

## I. Conclusion

Stage 7 successfully deployed **Apache Spark** and **Spark SQL** to perform high-performance, in-memory distributed analytics on the 51,656-record Zomato Bangalore restaurant dataset.

By leveraging PySpark DataFrames, the project executed robust schema transformations, regex-based type casting, and categorical feature engineering. Registering the in-memory view enabled the execution of eight comprehensive Spark SQL queries, revealing key urban dining patterns:
- **Commercial Concentration:** Extreme density in tech corridors like BTM and HSR.
- **Culinary Preferences:** Overwhelming market presence of North Indian and Chinese cuisines.
- **Economic Tiers:** Strong dominance of budget-friendly dining ($\le ₹500$).
- **Engagement Dynamics:** A verified moderate positive correlation ($r \approx 0.434$) between ratings and vote accumulation.

Stage 7 demonstrates the speed, expressiveness, and analytical power of Apache Spark in modern big data pipelines, successfully fulfilling the distributed computing objectives of the FoodieAnalytics project.

---

*Report prepared for FoodieAnalytics — Big Data Analytics Mini Project | Stage 7: Apache Spark & Spark SQL Analytics*
