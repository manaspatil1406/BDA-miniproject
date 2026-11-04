# FoodieAnalytics – Food Delivery Analytics and Restaurant Recommendation System

An end-to-end Big Data Analytics project analyzing 51,656 Bangalore restaurants using a full pipeline of distributed storage, batch processing, SQL analytics, machine learning, real-time streaming, and an interactive web dashboard.

---

## Project Stages

| Stage | Technology | Focus | Status |
|:------|:-----------|:------|:------:|
| 1 | Python, Pandas | Dataset exploration & schema auditing | ✅ Done |
| 2 | Python, Pandas | Data cleaning & preprocessing | ✅ Done |
| 3 | Hadoop 3.4.3, HDFS | Distributed storage on Ubuntu WSL2 | ✅ Done |
| 4 | Hadoop Streaming, Python | 4 MapReduce analytics jobs | ✅ Done |
| 5 | Apache Hive 4.0.1 | Data warehousing, 10 HiveQL queries | ✅ Done |
| 6 | — | NoSQL (skipped) | ⏭️ Skipped |
| 7 | Apache Spark 4.0.1 | Spark SQL — 8 analytical queries | ✅ Done |
| 8 | Spark MLlib | K-Means clustering (k=5) + recommendation engine | ✅ Done |
| 9 | Spark Structured Streaming | Simulated live order stream processing | ✅ Done |
| 10 | Streamlit, Plotly, Pandas | Interactive 6-page analytics dashboard | ✅ Done |

---

## Dataset

| Property | Value |
|:---------|:------|
| Source | Zomato Bangalore Restaurant Dataset |
| Raw records | 51,717 rows |
| After cleaning | 51,656 rows × 17 columns |
| Localities covered | 93 Bangalore neighborhoods |
| File | `dataset/processed/cleaned_zomato.csv` |

---

## Key Findings

| Metric | Value |
|:-------|:------|
| Most popular cuisine | North Indian (21,069 outlets) |
| Busiest locality | BTM (5,114 restaurants) |
| Highest rated locality | Lavelle Road (4.14 ★) |
| Most premium locality | Sankey Road (₹2,506 avg cost for two) |
| Average restaurant rating | 3.70 / 5.0 |
| Average cost for two | ₹555 |
| Online delivery adoption | 58.9% |
| Rating–votes correlation | Pearson r = +0.434 |
| K-Means clusters | 5 (Budget → Luxury → Brewery) |
| Simulated streaming orders | 50 orders · ₹50,282 GMV |

---

## Project Structure

```
BDA-miniproject/
├── app/                          # Stage 10 – Streamlit dashboard
│   ├── Home.py
│   ├── pages/
│   │   ├── 1_Restaurant_Analytics.py
│   │   ├── 2_Cuisine_Location_Analysis.py
│   │   ├── 3_Pricing_Delivery_Analytics.py
│   │   ├── 4_Restaurant_Recommendation.py
│   │   └── 5_Live_Order_Monitoring.py
│   ├── utils/
│   │   ├── data_loader.py
│   │   └── recommendation_engine.py
│   └── assets/styles.css
│
├── dataset/
│   ├── raw/zomato.csv            # Raw 574 MB dataset
│   ├── processed/                # Cleaned CSV, Parquet, feature vectors
│   └── streaming/                # Stage 9 JSON order batches & checkpoints
│
├── scripts/
│   ├── mapreduce/                # Stage 4 – 4 Hadoop Streaming jobs
│   ├── hive/                     # Stage 5 – SQL scripts & Beeline runner
│   ├── spark/                    # Stage 7 – Spark SQL & transformations
│   ├── recommendation/           # Stage 8 – MLlib pipeline
│   └── streaming/                # Stage 9 – order generator & consumer
│
├── models/recommendation/        # Serialized MLlib models (KMeans, encoders)
├── reports/                      # All analytical outputs (CSV, Parquet, TXT)
├── docs/                         # Stage documentation (Markdown)
├── documentation/                # Detailed per-stage technical reports
├── notebooks/                    # Jupyter notebooks (Stages 1 & 2)
├── preprocessing/clean_data.py   # Stage 2 cleaning pipeline
└── requirements.txt
```

---

## How to Run This Project

### Prerequisites

| Requirement | Version | Notes |
|:------------|:--------|:------|
| Windows with WSL2 | Ubuntu 24.04 LTS | All Hadoop/Hive/Spark runs inside WSL2 |
| Java (OpenJDK) | 21 | Required by Hadoop & Spark |
| Apache Hadoop | 3.4.3 | Installed in WSL2 |
| Apache Hive | 4.0.1 | Installed in WSL2 |
| Apache Spark | 4.0.1 | Via PySpark in virtualenv |
| Python | 3.12.3 | Inside WSL2 virtualenv |
| Virtualenv | `~/venvs/foodieanalytics-spark` | Pre-created with PySpark + dependencies |

> All commands below run **inside Ubuntu WSL2** unless stated otherwise.

---

### Step 1 – Clone and Enter the Project

```bash
# In WSL2
cd /mnt/c/
git clone <your-repo-url> BDA-miniproject
cd /mnt/c/BDA-miniproject
```

---

### Step 2 – Set Up Python Virtual Environment

```bash
# Create virtualenv (if not already created)
python3 -m venv ~/venvs/foodieanalytics-spark

# Activate it
source ~/venvs/foodieanalytics-spark/bin/activate

# Install all Python dependencies
pip install -r requirements.txt
```

---

### Step 3 – Data Cleaning (Stage 2)

```bash
source ~/venvs/foodieanalytics-spark/bin/activate
cd /mnt/c/BDA-miniproject
python3 preprocessing/clean_data.py
```

Produces `dataset/processed/cleaned_zomato.csv` (51,656 rows).

---

### Step 4 – Start Hadoop and Upload to HDFS (Stage 3)

```bash
# Start Hadoop daemons
start-dfs.sh
start-yarn.sh

# Upload dataset to HDFS
bash scripts/hdfs_integration/upload_to_hdfs.sh

# Verify
bash scripts/hdfs_integration/verify_hdfs.sh
```

Hadoop NameNode UI → `http://localhost:9870`

---

### Step 5 – Run MapReduce Jobs (Stage 4)

```bash
# Prepare Hadoop-safe CSV (run once)
python3 scripts/mapreduce/prepare_hadoop_safe_csv.py

# Upload to HDFS (run once)
hdfs dfs -put dataset/processed/hadoop_safe_zomato.csv /foodieanalytics/data/

# Run all 4 jobs
bash scripts/mapreduce/job1_restaurant_count/run_job.sh
bash scripts/mapreduce/job2_average_rating/run_job.sh
bash scripts/mapreduce/job3_cuisine_distribution/run_job.sh
bash scripts/mapreduce/job4_online_order_analysis/run_job.sh
```

Outputs saved to `reports/mapreduce_results/`.

---

### Step 6 – Run Hive Analytics (Stage 5)

```bash
# Hive requires Hadoop running (Step 4)
beeline -u 'jdbc:hive2:///' -f scripts/hive/create_database_table.sql
bash scripts/hive/run_all_queries.sh
```

Outputs saved to `reports/hive_results/`.

---

### Step 7 – Run Spark SQL Analytics (Stage 7)

```bash
source ~/venvs/foodieanalytics-spark/bin/activate
cd /mnt/c/BDA-miniproject

python3 scripts/spark/test_spark.py
python3 scripts/spark/load_restaurant_data.py
python3 scripts/spark/dataframe_transformations.py
python3 scripts/spark/spark_sql_analytics.py
```

Outputs saved to `reports/spark_results/query01/` through `query08/`.  
Spark UI → `http://localhost:4040` (while session is active)

---

### Step 8 – Run the Recommendation Pipeline (Stage 8)

```bash
source ~/venvs/foodieanalytics-spark/bin/activate
cd /mnt/c/BDA-miniproject

# Run pipeline steps in order
python3 scripts/recommendation/prepare_recommendation_data.py
python3 scripts/recommendation/cuisine_encoding.py
python3 scripts/recommendation/feature_vector.py
python3 scripts/recommendation/restaurant_clustering.py

# Run the interactive CLI recommender
python3 scripts/recommendation/recommend_restaurants.py
```

Models saved to `models/recommendation/`.  
Clustered data saved to `dataset/processed/clustered_restaurants/`.

---

### Step 9 – Run Spark Structured Streaming (Stage 9)

Open **two WSL2 terminal windows**:

**Terminal 1 – Order Generator:**
```bash
source ~/venvs/foodieanalytics-spark/bin/activate
cd /mnt/c/BDA-miniproject
python3 scripts/streaming/order_generator.py
```

**Terminal 2 – Streaming Consumer & Analytics:**
```bash
source ~/venvs/foodieanalytics-spark/bin/activate
cd /mnt/c/BDA-miniproject
python3 scripts/streaming/save_streaming_analytics.py
```

Outputs saved to `reports/streaming/` (Parquet + 3 CSV summaries).

---

### Step 10 – Launch the Interactive Dashboard (Stage 10)

```bash
source ~/venvs/foodieanalytics-spark/bin/activate
cd /mnt/c/BDA-miniproject
streamlit run app/Home.py
```

Open **http://localhost:8501** in your Windows browser.

| Page | What It Shows |
|:-----|:-------------|
| 🍔 Home | KPI cards, locality bar chart, cuisine chart, price tier donut |
| ⭐ Restaurant Analytics | Top-rated restaurants, rating extremes, Pearson r = 0.434, explorer |
| 🗺️ Cuisine & Location | Top 20 cuisines, density vs quality matrix, 93-locality deep-dive |
| 💳 Pricing & Delivery | Price tier breakdown, cost extremes, delivery adoption by locality |
| 🤖 Recommendation Engine | ML-powered recommendation (108 cuisines, 93 localities, Stage 8 K-Means) |
| ⚡ Live Order Monitoring | 50 simulated streaming orders, ₹50,282 GMV, 4 status breakdowns |

---

## Recommendation Scoring Formula (Stage 8 & 10)

$$\text{Score} = \left(0.40 \cdot S_{\text{cuisine}} + 0.25 \cdot S_{\text{rating}} + 0.15 \cdot S_{\text{budget}} + 0.10 \cdot S_{\text{delivery}} + 0.10 \cdot S_{\text{location}}\right) \times 100$$

| Component | Weight | Method |
|:----------|:------:|:-------|
| Cuisine match | 40% | 1.0 if restaurant passes cuisine filter |
| Rating quality | 25% | `rating / 5.0` (normalized) |
| Budget compatibility | 15% | `max(0, 1 − cost / budget)` |
| Delivery preference | 10% | 1.0 if delivery requirement satisfied |
| Location match | 10% | 1.0 if locality requirement satisfied |

---

## Documentation

| Stage | Document |
|:------|:---------|
| Stage 3 – HDFS | `documentation/hadoop_hdfs/` |
| Stage 4 – MapReduce | `documentation/mapreduce/stage4_execution_report.md` |
| Stage 5 – Hive | `documentation/hive/stage5_hive_analytics.md` |
| Stage 7 – Spark | `documentation/spark/STAGE_7_SPARK_ANALYTICS.md` |
| Stage 8 – MLlib | `documentation/recommendation/STAGE_8_DOCUMENTATION.md` |
| Stage 9 – Streaming | `docs/stage9_spark_streaming.md` |
| Stage 10 – Dashboard | `docs/stage10_dashboard.md` |

---

## Tech Stack

`Python 3.12` · `Apache Hadoop 3.4.3` · `Apache Hive 4.0.1` · `Apache Spark 4.0.1` · `PySpark MLlib` · `Spark Structured Streaming` · `Streamlit` · `Plotly` · `Pandas` · `PyArrow` · `Ubuntu 24.04 WSL2`
