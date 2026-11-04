# Stage 8 – Spark MLlib Restaurant Recommendation Scripts

This directory contains the PySpark MLlib implementation of the **Content-Based Restaurant Recommendation System** and **Unsupervised K-Means Clustering** for the FoodieAnalytics project.

---

## 1. Directory Contents & Script Purpose

| Script | Purpose | Execution Phase |
| :--- | :--- | :---: |
| [`prepare_recommendation_data.py`](prepare_recommendation_data.py) | Ingests `hadoop_safe_zomato.csv`, extracts numeric features, standardizes cuisine arrays and location strings, exports Parquet. | Step 1 & 2 |
| [`cuisine_encoding.py`](cuisine_encoding.py) | Identifies 89 primary cuisine categories; fits and applies `StringIndexer` and `OneHotEncoder` to generate 89-dimensional sparse vectors. | Step 3 |
| [`feature_vector.py`](feature_vector.py) | Applies `log1p` to votes, scales 5 numerical features with `StandardScaler`, and combines with cuisine vectors via `VectorAssembler` into 94-dim feature vector. | Step 4 |
| [`restaurant_clustering.py`](restaurant_clustering.py) | Fits PySpark MLlib `KMeans` ($k=5$, maxIter=20, seed=42), assigns `cluster_id`, computes cluster statistics, and saves trained model. | Step 5 |
| [`recommend_restaurants.py`](recommend_restaurants.py) | Interactive CLI engine: collects user preferences, filters candidates, calculates 5-component weighted recommendation scores, and exports Top 10 to CSV. | Step 6 |
| [`diagnose_filters.py`](diagnose_filters.py) | Diagnostic utility to inspect candidate counts across sequential filter criteria. | Diagnostic |

---

## 2. Environment & Prerequisites

### Required Technologies
- **Operating System:** Ubuntu 24.04.5 LTS on WSL2 (`/mnt/c/BDA-miniproject`)
- **Python:** Python 3.12+
- **Java:** OpenJDK 21 (`/usr/lib/jvm/java-21-openjdk-amd64`)
- **Engine:** Apache Spark / PySpark 4.0.1
- **Dedicated Virtual Environment:** `~/venvs/foodieanalytics-spark`

### Environment Activation
```bash
source ~/venvs/foodieanalytics-spark/bin/activate
```

### Prerequisites
- Single-line-safe dataset must exist:
  `/mnt/c/BDA-miniproject/dataset/processed/hadoop_safe_zomato.csv` (51,656 restaurant records).

---

## 3. Execution Order & WSL2 Commands

Run the scripts in strict sequential order from the project root directory:

```bash
# Activate environment and navigate to project root
source ~/venvs/foodieanalytics-spark/bin/activate
cd /mnt/c/BDA-miniproject

# Step 1 & 2: Prepare Recommendation Dataset
python3 scripts/recommendation/prepare_recommendation_data.py

# Step 3: Categorical Cuisine Encoding
python3 scripts/recommendation/cuisine_encoding.py

# Step 4: Feature Vector Assembly & Scaling
python3 scripts/recommendation/feature_vector.py

# Step 5: Unsupervised Restaurant Clustering (K-Means)
python3 scripts/recommendation/restaurant_clustering.py

# Step 6: Interactive Recommendation Engine
python3 scripts/recommendation/recommend_restaurants.py
```

---

## 4. Input and Output Dataset Paths

| Stage / Step | Input Dataset | Output Dataset |
| :--- | :--- | :--- |
| **Data Preparation** | `dataset/processed/hadoop_safe_zomato.csv` | `dataset/processed/recommendation_ready/` (Parquet) |
| **Cuisine Encoding** | `dataset/processed/recommendation_ready/` | `dataset/processed/cuisine_encoded/` (Parquet) |
| **Feature Vector** | `dataset/processed/cuisine_encoded/` | `dataset/processed/feature_vectors/` (Parquet) |
| **Clustering** | `dataset/processed/feature_vectors/` | `dataset/processed/clustered_restaurants/` (Parquet) |
| **Recommendation** | `dataset/processed/clustered_restaurants/` | `reports/recommendations/` (CSV) |

---

## 5. Model Storage Paths

All trained Spark MLlib models are serialized in native Spark ML format:

| Model Component | Model Type | Disk Path |
| :--- | :--- | :--- |
| **String Indexer** | `StringIndexerModel` (89 labels) | `models/recommendation/cuisine_encoder/string_indexer/` |
| **One-Hot Encoder** | `OneHotEncoderModel` (89 dimensions) | `models/recommendation/cuisine_encoder/one_hot_encoder/` |
| **Numerical Scaler** | `StandardScalerModel` (5 dimensions) | `models/recommendation/numeric_scaler/` |
| **K-Means Clusterer** | `KMeansModel` ($k=5$ centroids) | `models/recommendation/kmeans_model/` |

---

## 6. Recommendation Engine Usage

When running `recommend_restaurants.py`, you will be prompted for 5 interactive inputs:

```text
Enter your restaurant preferences:

Preferred cuisine (e.g. Chinese, Italian, North Indian): Chinese
Preferred Bangalore locality (e.g. BTM, HSR, Whitefield): BTM
Maximum budget for two (e.g. 1000): 1000
Minimum restaurant rating (e.g. 4.0): 4.0
Online delivery required? (yes/no/any): any
```

### Scoring Formula Breakdown
Candidates surviving hard filtering are scored using:
$$\text{Score} = (0.40 \cdot S_{\text{cuisine}} + 0.25 \cdot S_{\text{rating}} + 0.15 \cdot S_{\text{budget}} + 0.10 \cdot S_{\text{delivery}} + 0.10 \cdot S_{\text{location}}) \times 100$$

- **Cuisine ($40\%$), Location ($10\%$), Delivery ($10\%$):** Evaluated as **fixed match scores ($1.0$)** post-filtering.
- **Rating Quality ($25\%$):** $\min(1.0, \max(0.0, \text{rating\_num} / 5.0))$.
- **Budget Compatibility ($15\%$):** $\max(0.0, 1.0 - (\text{cost\_for\_two\_num} / \text{budget}))$.

Results are exported as a single CSV partition to `reports/recommendations/`.

---

## 7. Troubleshooting & Spark Memory Optimization

When training Spark MLlib models locally on WSL2, apply the following optimizations:

1. **Driver Memory Allocation:**  
   Configure `.config("spark.driver.memory", "2g")` to prevent Java heap space (`OutOfMemoryError`) during model fitting.
2. **Exclusion of High-Memory Text Columns:**  
   In `cuisine_encoding.py` and `feature_vector.py`, columns such as `reviews_list` and `menu_item` are explicitly excluded using `.select(*required_columns)` before model fitting. This reduces memory pressure by >80%.
3. **Shuffle Partition Tuning:**  
   Set `.config("spark.sql.shuffle.partitions", "4")` for local mode execution. The default of 200 partitions creates unnecessary task scheduling overhead for single-node workloads.
4. **Disabling Vectorized Parquet Reader:**  
   Set `.config("spark.sql.parquet.enableVectorizedReader", "false")`. Complex Spark ML vector representations (`VectorUDT`) can trigger JVM segmentation faults in certain native Parquet readers on WSL2.
5. **Single-File Output Export:**  
   In `recommend_restaurants.py`, use `.coalesce(1)` before `.write.csv()` to ensure the Top 10 recommendations are exported as a single readable CSV file rather than multiple partitioned parts.

