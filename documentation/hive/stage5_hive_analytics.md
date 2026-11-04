# Stage 5 – Apache Hive Analytics: Execution Report

**Project:** FoodieAnalytics — Food Delivery Analytics and Restaurant Recommendation System  
**Stage:** 5 – SQL Analytics with Apache Hive  
**Dataset:** Zomato Bangalore Restaurant Dataset (51,656 records, 17 columns)  
**Environment:** Ubuntu 24.04.5 LTS on WSL2, Apache Hive 4.2.1, Hadoop 3.4.3, Java 21

---

## 1. Aim

To perform structured analytical queries on the Zomato restaurant dataset using **Apache Hive**, enabling SQL-based Big Data analytics on data stored in the Hadoop Distributed File System (HDFS). The goal is to derive business insights about Bangalore's restaurant ecosystem across ten analytical dimensions.

---

## 2. Introduction to Apache Hive

Apache Hive is a data warehouse infrastructure built on top of Apache Hadoop. It provides an SQL-like query language called **HiveQL** that allows users to run queries on large datasets stored in HDFS without needing to write low-level MapReduce programs.

Hive translates HiveQL queries into MapReduce, Tez, or Spark jobs depending on the configured execution engine. It introduces the concept of a **schema on read** — meaning data is stored as plain files in HDFS, and the schema is applied only when the data is read for a query. This makes Hive extremely flexible for analytics on structured and semi-structured data.

Key components:
- **HiveQL**: An SQL dialect that supports SELECT, GROUP BY, ORDER BY, JOIN, LATERAL VIEW EXPLODE, and more.
- **Metastore**: A relational database (Derby in embedded mode, MySQL/PostgreSQL in production) that stores table schema metadata.
- **Driver**: Parses and compiles HiveQL, generates execution plans.
- **Execution Engine**: Runs the plan — Hive-on-MapReduce (default), Tez, or Spark.
- **SerDe (Serializer/Deserializer)**: Reads and writes data in various formats (CSV, JSON, ORC, Parquet).
- **Beeline**: The JDBC command-line client used to connect to HiveServer2.

---

## 3. Why Hive is Used in This Project

| Need | Hive Solution |
|------|--------------|
| SQL familiarity for analytics | HiveQL is near-identical to standard SQL |
| Data already in HDFS | External tables point directly to HDFS files |
| Structured aggregations | GROUP BY, AVG, COUNT, ORDER BY natively supported |
| Multi-valued field expansion | `LATERAL VIEW EXPLODE(SPLIT(...))` for cuisine analysis |
| No data movement required | Schema-on-read — no ETL import step |
| Academic demonstration | Hive is a core Big Data analytics tool in industry |

---

## 4. Role of Hive in Big Data Analytics

In the context of the full FoodieAnalytics pipeline:

```
Raw CSV (Windows)
      │
      ▼
Stage 1–2: Python data cleaning → cleaned_zomato.csv
      │
      ▼
Stage 3: Hadoop HDFS → /foodieanalytics/data/cleaned_zomato.csv
      │
      ▼
Stage 4: Hadoop MapReduce → distributed counting & aggregation (Python)
      │
      ▼
Stage 5: Apache Hive → SQL analytics on /foodieanalytics/hive_input/
      │
      ▼
      Analytical insights → reports/hive_results/
```

Hive sits above HDFS and MapReduce in the stack, providing:
- **Declarative queries**: Analysts write what they want, not how to compute it.
- **Reusable schema**: The `restaurants` table is defined once and queried repeatedly.
- **Richer analytics**: Multi-table joins, window functions, lateral views — far beyond simple MapReduce word-count patterns.

---

## 5. Hive Architecture

```
    ┌──────────────────────────────────────────────────────────┐
    │                      BEELINE CLIENT                      │
    │       (JDBC connection to HiveServer2 via localhost)     │
    └───────────────────────┬──────────────────────────────────┘
                            │  HiveQL query
                            ▼
    ┌──────────────────────────────────────────────────────────┐
    │                     HIVESERVER 2                         │
    │  ┌─────────┐  ┌──────────┐  ┌────────────────────────┐  │
    │  │ Driver  │  │ Compiler │  │   Execution Engine     │  │
    │  │(Parser) │  │(Planner) │  │   (Hive-on-MR / Tez)  │  │
    │  └─────────┘  └──────────┘  └────────────────────────┘  │
    └──────────────────────┬───────────────────────────────────┘
                           │  metadata lookups
                           ▼
    ┌──────────────────────────────────────────────────────────┐
    │                      METASTORE                           │
    │        (Apache Derby – embedded mode)                    │
    │   Stores: database names, table names, column types,     │
    │            SerDe configuration, HDFS location           │
    └──────────────────────┬───────────────────────────────────┘
                           │  read/write data
                           ▼
    ┌──────────────────────────────────────────────────────────┐
    │                        HDFS                              │
    │   /foodieanalytics/hive_input/hadoop_safe_zomato.csv     │
    │   NameNode: hdfs://localhost:9000                        │
    └──────────────────────────────────────────────────────────┘
```

**Data flow for a query:**
1. Beeline sends HiveQL to HiveServer2.
2. Driver parses the query and looks up table metadata from Derby Metastore.
3. Compiler generates an execution plan (MapReduce job or local execution).
4. Execution engine reads data blocks from HDFS, applying the OpenCSVSerde to parse fields.
5. Results are streamed back to Beeline and written to the output file.

---

## 6. Environment and Configuration

| Component          | Details                                                   |
|--------------------|-----------------------------------------------------------|
| Operating System   | Ubuntu 24.04.5 LTS (WSL2 on Windows)                    |
| Java               | OpenJDK 21                                               |
| Hadoop             | 3.4.3 (NameNode at `hdfs://localhost:9000`)              |
| Apache Hive        | 4.2.1 (`/home/manas_patil/apps/apache-hive-4.2.1-bin`)  |
| Hive Metastore     | Apache Derby (embedded, local mode)                      |
| Hive Client        | Beeline (JDBC: `jdbc:hive2:///`)                         |
| HDFS Input Path    | `/foodieanalytics/hive_input/`                           |
| Input File         | `hadoop_safe_zomato.csv` (51,657 lines, 1 header)        |
| Terminal Fix       | `JAVA_TOOL_OPTIONS=-Dorg.jline.terminal.type=dumb`       |

**Key environment variables set in Ubuntu WSL2:**
```bash
export HADOOP_HOME=/home/manas_patil/apps/hadoop-3.4.3
export HIVE_HOME=/home/manas_patil/apps/apache-hive-4.2.1-bin
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$JAVA_HOME/bin:$PATH
export JAVA_TOOL_OPTIONS="-Dorg.jline.terminal.providers=dumb -Dorg.jline.terminal.type=dumb"
```

---

## 7. Steps Performed

### Step 1: Hive Installation

Downloaded and extracted Apache Hive 4.2.1 binary tarball to `/home/manas_patil/apps/apache-hive-4.2.1-bin`. Set `HIVE_HOME` and added `$HIVE_HOME/bin` to `PATH`.

### Step 2: Hive Configuration

Configured `hive-site.xml` to use Derby as the embedded metastore. Set the warehouse directory to `/user/hive/warehouse` in HDFS.

### Step 3: Metastore Schema Initialization

Initialized the Derby metastore schema using:
```bash
schematool -dbType derby -initSchema
```
This creates the Derby database files in the working directory and sets up all Hive metadata tables.

### Step 4: HDFS Warehouse and Input Directory Setup

Created the required HDFS directories:
```bash
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chmod g+w /user/hive/warehouse
hdfs dfs -mkdir -p /foodieanalytics/hive_input
```

### Step 5: Dataset Upload to HDFS Hive Input Directory

Uploaded the newline-safe dataset to the Hive input path:
```bash
hdfs dfs -put dataset/processed/hadoop_safe_zomato.csv /foodieanalytics/hive_input/
hdfs dfs -ls /foodieanalytics/hive_input/
```

### Step 6: HiveServer2 Startup

Started the HiveServer2 daemon (runs in background):
```bash
hiveserver2 &
```
Waited ~10 seconds for it to initialize before connecting with Beeline.

### Step 7: Restaurant Database and External Table Creation

Connected with Beeline and executed `create_database_table.sql`:
```bash
beeline -u 'jdbc:hive2:///' -f scripts/hive/create_database_table.sql
```

This created:
- Database: `foodieanalytics`
- External table: `restaurants` pointing to `/foodieanalytics/hive_input/`
- SerDe: `OpenCSVSerde` with `,` separator and `"` quote character
- Header skipped: `skip.header.line.count = 1`

### Step 8: Execution of Ten Analytical SQL Queries

Ran all ten queries using the batch script:
```bash
bash scripts/hive/run_all_queries.sh
```

Each query was executed via Beeline with `--silent=true --outputformat=tsv2` and results redirected to the corresponding `.txt` file in `reports/hive_results/`.

### Step 9: Result Verification

Retrieved results from all ten output files and verified that each contained actual tab-separated data rows, not just error messages or empty output.

---

## 8. Database and Table Schema

### Database

```sql
CREATE DATABASE IF NOT EXISTS foodieanalytics;
```

### External Table

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS restaurants (
    url                       STRING,
    address                   STRING,
    name                      STRING,
    online_order              STRING,
    book_table                STRING,
    rate                      STRING,
    votes                     STRING,
    phone                     STRING,
    location                  STRING,
    rest_type                 STRING,
    dish_liked                STRING,
    cuisines                  STRING,
    approx_cost_for_two_people STRING,
    reviews_list              STRING,
    menu_item                 STRING,
    listed_in_type            STRING,
    listed_in_city            STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\"",
    "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/foodieanalytics/hive_input'
TBLPROPERTIES ("skip.header.line.count" = "1");
```

### Column Reference

| # | Column Name                 | Hive Type | Description                                    |
|---|-----------------------------|-----------|------------------------------------------------|
| 1 | `url`                       | STRING    | Unique Zomato restaurant page URL              |
| 2 | `address`                   | STRING    | Full street address                            |
| 3 | `name`                      | STRING    | Restaurant name                                |
| 4 | `online_order`              | STRING    | Online ordering availability (`True`/`False`)  |
| 5 | `book_table`                | STRING    | Table reservation availability (`True`/`False`)|
| 6 | `rate`                      | STRING    | Rating string — cast to DOUBLE in queries      |
| 7 | `votes`                     | STRING    | Total rating votes — cast to BIGINT in queries |
| 8 | `phone`                     | STRING    | Contact phone number                           |
| 9 | `location`                  | STRING    | Locality / neighbourhood name                  |
| 10| `rest_type`                 | STRING    | Restaurant format (Quick Bites, Casual Dining…)|
| 11| `dish_liked`                | STRING    | Comma-separated popular dishes                 |
| 12| `cuisines`                  | STRING    | Comma-separated cuisine types                  |
| 13| `approx_cost_for_two_people`| STRING    | Approximate cost for two in INR                |
| 14| `reviews_list`              | STRING    | Customer review text (embedded in CSV)         |
| 15| `menu_item`                 | STRING    | Menu items list                                |
| 16| `listed_in_type`            | STRING    | Listing category (Delivery, Dine-out, Cafes…) |
| 17| `listed_in_city`            | STRING    | Delivery zone / city sector                    |

> **Note:** All columns are declared as `STRING` because `OpenCSVSerde` returns all fields as strings. Numeric types (`rate`, `votes`, `approx_cost_for_two_people`) are explicitly cast using `CAST(... AS DOUBLE)` or `CAST(... AS BIGINT)` within each query, with invalid values (`'NEW'`, `'-'`, `''`) filtered out by `WHERE` clauses.

---

## 9. Query Summary Table

| Query No. | Analytical Operation          | SQL File                            | Output File                            |
|:---------:|-------------------------------|-------------------------------------|----------------------------------------|
| 1         | Top-rated restaurants         | `query01_top_rated.sql`             | `query01_top_rated.txt`               |
| 2         | Restaurant count by locality  | `query02_restaurants_by_locality.sql`| `query02_restaurants_by_locality.txt` |
| 3         | Popular cuisines              | `query03_popular_cuisines.sql`      | `query03_popular_cuisines.txt`        |
| 4         | Online ordering by locality   | `query04_online_ordering.sql`       | `query04_online_ordering.txt`         |
| 5         | Average rating by locality    | `query05_average_rating.sql`        | `query05_average_rating.txt`          |
| 6         | Top restaurants by votes      | `query06_top_voted.sql`             | `query06_top_voted.txt`               |
| 7         | Average cost by locality      | `query07_average_cost.sql`          | `query07_average_cost.txt`            |
| 8         | Restaurant type distribution  | `query08_restaurant_type.sql`       | `query08_restaurant_type.txt`         |
| 9         | Table booking by locality     | `query09_table_booking.sql`         | `query09_table_booking.txt`           |
| 10        | Average rating by category    | `query10_rating_by_category.sql`    | `query10_rating_by_category.txt`      |

---

## 10. Query Details and Findings

### Query 1 – Top-Rated Restaurants

**File:** `scripts/hive/queries/query01_top_rated.sql`  
**Objective:** Find the top 10 highest-rated restaurants by star rating (1.0–5.0 scale).

**Key SQL technique:** `CAST(rate AS DOUBLE)` with range validation and `ORDER BY rating DESC LIMIT 10`.

**Results (actual output):**

| Restaurant Name                   | Location              | Rating |
|-----------------------------------|-----------------------|:------:|
| Flechazo                          | Whitefield            | 4.9    |
| Byg Brewski Brewing Company       | Sarjapur Road         | 4.9    |
| Belgian Waffle Factory            | Brigade Road          | 4.9    |
| Asia Kitchen By Mainland China    | Koramangala 5th Block | 4.9    |
| AB's - Absolute Barbecues         | BTM                   | 4.9    |

**Finding:** The highest achievable rating in the dataset is **4.9/5.0**. Multiple listings of the same restaurant appear because Zomato lists restaurants under multiple delivery zones.

---

### Query 2 – Restaurant Count by Locality

**File:** `scripts/hive/queries/query02_restaurants_by_locality.sql`  
**Objective:** Count total restaurants in each Bangalore locality, ordered by density.

**Key SQL technique:** `GROUP BY location ORDER BY COUNT(*) DESC`.

**Top results (actual output):**

| Locality              | Restaurant Count |
|-----------------------|:----------------:|
| BTM                   | 5,114            |
| HSR                   | 2,522            |
| Koramangala 5th Block | 2,504            |
| JP Nagar              | 2,234            |
| Whitefield            | 2,142            |
| Indiranagar           | 2,082            |
| Jayanagar             | 1,926            |
| Marathahalli          | 1,846            |
| Bannerghatta Road     | 1,630            |
| Bellandur             | 1,286            |

**Finding:** BTM dominates with **5,114 restaurants** — more than twice the second-ranked HSR (2,522). BTM's dense residential and commercial character drives this.

---

### Query 3 – Popular Cuisines

**File:** `scripts/hive/queries/query03_popular_cuisines.sql`  
**Objective:** Identify the top 10 most frequently offered cuisines across all restaurants.

**Key SQL technique:** `LATERAL VIEW EXPLODE(SPLIT(cuisines, ','))` to unnest the comma-separated cuisines field, followed by `GROUP BY` and `ORDER BY COUNT(*) DESC LIMIT 10`.

**Results (actual output — all 10 rows):**

| Rank | Cuisine       | Restaurant Count |
|:----:|---------------|:----------------:|
| 1    | North Indian  | 21,069           |
| 2    | Chinese       | 15,534           |
| 3    | South Indian  | 8,638            |
| 4    | Fast Food     | 8,087            |
| 5    | Biryani       | 6,483            |
| 6    | Continental   | 5,759            |
| 7    | Desserts      | 5,631            |
| 8    | Cafe          | 5,300            |
| 9    | Beverages     | 4,743            |
| 10   | Italian       | 3,386            |

**Finding:** North Indian (21,069) and Chinese (15,534) together account for over 36,000 entries — more than South Indian, Fast Food, and Biryani combined — reflecting Bangalore's cosmopolitan, pan-Indian food culture.

---

### Query 4 – Online Ordering Availability by Locality

**File:** `scripts/hive/queries/query04_online_ordering.sql`  
**Objective:** Count restaurants per locality that offer online ordering.

**Key SQL technique:** `WHERE LOWER(TRIM(online_order)) = 'true'` with `GROUP BY location ORDER BY COUNT(*) DESC`.

**Top 10 (actual output):**

| Locality              | Online Order Count |
|-----------------------|:-----------------:|
| BTM                   | 3,325             |
| HSR                   | 1,920             |
| Koramangala 5th Block | 1,614             |
| Jayanagar             | 1,364             |
| Indiranagar           | 1,335             |
| JP Nagar              | 1,317             |
| Whitefield            | 1,137             |
| Marathahalli          | 1,112             |
| Bannerghatta Road     | 924               |
| Koramangala 1st Block | 852               |

**Finding:** BTM's 3,325 online-ordering restaurants is the highest absolute count, driven by its sheer restaurant density. The ranking here reflects total count, not adoption rate — HSR has a higher adoption percentage relative to its total (76%) despite ranking second by absolute count.

---

### Query 5 – Average Rating by Locality

**File:** `scripts/hive/queries/query05_average_rating.sql`  
**Objective:** Compute average restaurant rating and rated restaurant count per locality, ordered by quality.

**Key SQL technique:** `ROUND(AVG(CAST(rate AS DOUBLE)), 2)` with invalid rate filtering and `GROUP BY location ORDER BY average_rating DESC`.

**Top 10 (actual output):**

| Locality                | Avg Rating | Rated Count |
|-------------------------|:----------:|:-----------:|
| Lavelle Road            | 4.14       | 485         |
| St. Marks Road          | 4.02       | 343         |
| Koramangala 3rd Block   | 4.02       | 191         |
| Koramangala 5th Block   | 4.01       | 2,319       |
| Church Street           | 3.99       | 546         |
| Sankey Road             | 3.97       | 26          |
| Koramangala 4th Block   | 3.92       | 841         |
| Cunningham Road         | 3.90       | 475         |
| Residency Road          | 3.86       | 604         |
| MG Road                 | 3.86       | 811         |

**Finding:** **Lavelle Road** leads with an average rating of **4.14** — the highest of any locality — suggesting it hosts Bangalore's highest concentration of premium, well-regarded restaurants. The Koramangala 5th Block entry is notable: despite being high-volume (2,319 rated restaurants), it maintains an excellent average of 4.01.

---

### Query 6 – Top Restaurants by Number of Votes

**File:** `scripts/hive/queries/query06_top_voted.sql`  
**Objective:** Find the 10 most-reviewed restaurants by total customer vote count.

**Key SQL technique:** `CAST(votes AS BIGINT)` with `ORDER BY total_votes DESC LIMIT 10`.

**Results (actual output — all 10 rows):**

| Restaurant Name              | Location              | Total Votes |
|------------------------------|-----------------------|:-----------:|
| Byg Brewski Brewing Company  | Sarjapur Road         | 16,832      |
| Byg Brewski Brewing Company  | Sarjapur Road         | 16,832      |
| Byg Brewski Brewing Company  | Sarjapur Road         | 16,832      |
| Byg Brewski Brewing Company  | Sarjapur Road         | 16,345      |
| Byg Brewski Brewing Company  | Sarjapur Road         | 16,345      |
| Byg Brewski Brewing Company  | Sarjapur Road         | 16,345      |
| Toit                         | Indiranagar           | 14,956      |
| Toit                         | Indiranagar           | 14,956      |
| Truffles                     | Koramangala 5th Block | 14,726      |
| Truffles                     | Koramangala 5th Block | 14,723      |

**Finding:** **Byg Brewski Brewing Company** on Sarjapur Road is by far the most-reviewed restaurant in Bangalore, with over 16,000 votes across multiple listings. **Toit** (Indiranagar) and **Truffles** (Koramangala) are the other two iconic crowd-favorites, reflecting Bangalore's vibrant brewery and casual dining culture.

---

### Query 7 – Average Cost for Two by Locality

**File:** `scripts/hive/queries/query07_average_cost.sql`  
**Objective:** Compute average meal cost for two people per locality, identifying premium vs. budget areas.

**Key SQL technique:** `ROUND(AVG(CAST(approx_cost_for_two_people AS DOUBLE)), 2)` with zero/null cost filtering and `GROUP BY location ORDER BY average_cost_for_two DESC`.

**Top 10 (actual output):**

| Locality         | Avg Cost for Two (₹) |
|------------------|:--------------------:|
| Sankey Road      | 2,505.56             |
| Race Course Road | 1,309.35             |
| Lavelle Road     | 1,307.97             |
| MG Road          | 1,155.70             |
| Infantry Road    | 1,060.67             |
| Residency Road   | 964.78               |
| Langford Town    | 883.33               |
| St. Marks Road   | 871.31               |
| Cunningham Road  | 864.97               |
| Church Street    | 834.89               |

**Finding:** **Sankey Road** is by far the most expensive dining locality (avg ₹2,505 for two), reflecting its proximity to premium venues and clubs. The Central Bangalore localities (MG Road, Lavelle Road, Church Street) consistently rank among the most expensive, while peripheral residential areas offer more affordable options.

---

### Query 8 – Restaurant Type Distribution

**File:** `scripts/hive/queries/query08_restaurant_type.sql`  
**Objective:** Count restaurants by restaurant format/type, ordered by frequency (top 15).

**Key SQL technique:** `GROUP BY rest_type ORDER BY COUNT(*) DESC LIMIT 15`.

**Results (actual output — all 15 rows):**

| Restaurant Type        | Count  |
|------------------------|:------:|
| Quick Bites            | 19,115 |
| Casual Dining          | 10,323 |
| Cafe                   | 3,730  |
| Delivery               | 2,600  |
| Dessert Parlor         | 2,263  |
| Takeaway, Delivery     | 2,034  |
| Casual Dining, Bar     | 1,154  |
| Bakery                 | 1,140  |
| Beverage Shop          | 866    |
| Bar                    | 697    |
| Food Court             | 623    |
| Sweet Shop             | 468    |
| Bar, Casual Dining     | 424    |
| Lounge                 | 396    |
| Pub                    | 356    |

**Finding:** **Quick Bites** (19,115) and **Casual Dining** (10,323) together account for about 57% of all restaurants — confirming Bangalore's market is dominated by fast, affordable meals rather than fine dining. The presence of multi-type entries (e.g., "Casual Dining, Bar") reflects how Zomato allows composite type tags.

---

### Query 9 – Table Booking Availability by Locality

**File:** `scripts/hive/queries/query09_table_booking.sql`  
**Objective:** Count restaurants per locality that offer table reservations.

**Key SQL technique:** `WHERE LOWER(TRIM(book_table)) = 'true'` with `GROUP BY location ORDER BY COUNT(*) DESC`.

**Top 10 (actual output):**

| Locality              | Table Booking Count |
|-----------------------|:------------------:|
| Koramangala 5th Block | 692                |
| Indiranagar           | 454                |
| MG Road               | 347                |
| Koramangala 4th Block | 322                |
| JP Nagar              | 315                |
| Jayanagar             | 279                |
| Whitefield            | 253                |
| Lavelle Road          | 231                |
| HSR                   | 217                |
| Church Street         | 182                |

**Finding:** **Koramangala 5th Block** leads with 692 table-booking restaurants, well ahead of Indiranagar (454). These are both premium, cosmopolitan localities. Notably, BTM — despite having the most restaurants overall (5,114) — has only 167 table-booking restaurants, confirming its Quick Bites / budget character.

---

### Query 10 – Average Rating by Listing Category

**File:** `scripts/hive/queries/query10_rating_by_category.sql`  
**Objective:** Compute average rating per Zomato listing category to identify which dining formats have the highest customer satisfaction.

**Key SQL technique:** `GROUP BY listed_in_type ORDER BY average_rating DESC` on `listed_in_type` column.

**Results (actual output — all 7 rows):**

| Listing Category   | Avg Rating | Rated Records |
|--------------------|:----------:|:-------------:|
| Pubs and bars      | 4.02       | 640           |
| Drinks & nightlife | 4.02       | 1,055         |
| Buffet             | 3.98       | 854           |
| Cafes              | 3.87       | 1,526         |
| Desserts           | 3.78       | 2,732         |
| Dine-out           | 3.68       | 14,203        |
| Delivery           | 3.65       | 20,630        |

**Finding:** **Pubs, bars, and nightlife venues** have the highest average ratings (4.02) — suggesting that Bangaloreans rate evening social venues more highly than daytime eateries. **Delivery-only** restaurants score lowest (3.65), which is consistent with the global observation that food quality degrades in transit compared to dine-in experience.

---

## 11. Challenges and Resolutions

### Challenge 1: Beeline / JLine Terminal Compatibility Issue

**Problem:** When running Beeline in a non-interactive terminal (redirecting output with `> file.txt`), Beeline 4.2.1 uses the JLine library to detect terminal capabilities. In WSL2 with output redirection, JLine fails to initialize the terminal, causing the error:
```
ERROR: Failed to initialize terminal: ...
```
or producing garbled/empty output instead of query results.

**Resolution:** Set the JLine terminal type to `dumb` before invoking Beeline:
```bash
export JAVA_TOOL_OPTIONS="-Dorg.jline.terminal.providers=dumb -Dorg.jline.terminal.type=dumb"
```
This forces JLine to use a simple dumb terminal that works correctly with output redirection. This setting is set in `run_all_queries.sh` before the query loop.

---

### Challenge 2: OpenCSVSerde – All Columns as STRING

**Problem:** `OpenCSVSerde` returns every field as a STRING regardless of the declared Hive type. Using `rate DOUBLE` in the table definition still resulted in strings — numeric operations failed.

**Resolution:** Declared all columns as `STRING` in the table schema and used explicit `CAST(rate AS DOUBLE)` / `CAST(votes AS BIGINT)` in each query's SELECT and WHERE clauses. Invalid values (`'NEW'`, `'-'`, `''`) were filtered with `TRIM(rate) NOT IN ('', 'NEW', '-')` before casting.

---

### Challenge 3: Embedded Newlines in `reviews_list`

**Problem:** The `cleaned_zomato.csv` file contains embedded newlines within quoted fields (`reviews_list` column), causing Hive's line reader to split single records across multiple lines. This produced incorrect row counts and corrupted field values.

**Resolution:** Used `hadoop_safe_zomato.csv` (generated in Stage 4 by `prepare_hadoop_safe_csv.py`) as the Hive input file. This file has exactly one physical line per logical record, making it fully compatible with Hive's default text file processing.

---

### Challenge 4: Multiple SLF4J Bindings Warning

**Problem:** Both Hive and Hadoop ship their own SLF4J logging bindings, causing warning messages:
```
SLF4J: Class path contains multiple SLF4J bindings.
```
These appear in every query output file alongside actual results.

**Resolution:** These are harmless warnings that do not affect query execution. They are mixed into the output because `stderr` and `stdout` are both redirected to the result files (`2>&1`). The actual query data (tab-separated rows starting with the column header) is identifiable by its format and can be extracted cleanly.

---

## 12. Advantages of Using Hive for This Project

1. **SQL familiarity**: Analysts familiar with MySQL/PostgreSQL can write HiveQL without learning MapReduce APIs.
2. **Schema reuse**: The `restaurants` table is defined once; all 10 queries reuse the same schema definition.
3. **Powerful built-ins**: `LATERAL VIEW EXPLODE`, `ROUND`, `AVG`, `GROUP BY`, `ORDER BY` — all natively available.
4. **HDFS integration**: External tables point directly to HDFS files — no data copying or import step required.
5. **Scalability**: The same HiveQL queries would work on a 100-node cluster with terabytes of data, not just this single-node setup.
6. **Auditability**: SQL queries are human-readable and easily reviewable compared to MapReduce code.

---

## 13. Limitations

1. **Hive-on-MR latency**: Since Hive 4.2.1 defaults to MapReduce execution, each query takes 1–2 minutes even on 51,656 records. Production deployments use Tez or Spark for 10–100× faster execution.
2. **No ACID transactions**: Hive's default ORC tables support ACID, but plain external CSV tables (as used here) are read-only and do not support UPDATE or DELETE.
3. **Derby Metastore**: The embedded Derby database is single-user; it cannot handle concurrent Beeline sessions. Production uses a shared MySQL or PostgreSQL metastore.
4. **OpenCSVSerde limitations**: Only handles simple CSV; complex nested structures (like the `reviews_list` column) are treated as a single string column.
5. **Column type casting overhead**: The need to `CAST` every numeric field in every query adds verbosity and execution overhead compared to properly typed tables.

---

## 14. Conclusion

Stage 5 successfully demonstrated SQL-based Big Data analytics using Apache Hive 4.2.1 on the Zomato Bangalore restaurant dataset. All ten analytical queries were executed via Beeline and produced verified, real output.

**Key analytical achievements:**
- Identified BTM as Bangalore's most restaurant-dense locality (5,114 restaurants).
- Confirmed North Indian and Chinese as the two dominant cuisines.
- Revealed Lavelle Road as the highest-rated locality (avg 4.14) and Sankey Road as the most expensive (avg ₹2,506 for two).
- Demonstrated that Pubs/Bars receive higher ratings than delivery-focused restaurants.
- Showed Byg Brewski Brewing Company as Bangalore's most-reviewed restaurant (16,832 votes).

The Hive layer completes the SQL analytics tier of the FoodieAnalytics Big Data pipeline, sitting above HDFS and MapReduce to provide structured, repeatable, and scalable analytical queries.

---

*Report generated for FoodieAnalytics – Stage 5 | Apache Hive 4.2.1 | Hadoop 3.4.3 | Ubuntu 24.04.5 WSL2*

