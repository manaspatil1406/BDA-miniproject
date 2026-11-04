# Stage 9 – Spark Structured Streaming Scripts

This directory contains the Apache Spark Structured Streaming implementation for simulated live food delivery order processing in the **FoodieAnalytics** project.

---

## 1. Directory Contents

| Script | Purpose | Execution Mode |
| :--- | :--- | :---: |
| [`order_generator.py`](order_generator.py) | Generates 10 batches of 5 synthetic orders (50 orders total) at 5-second intervals as JSON files in `dataset/streaming/orders_input/`. | Producer |
| [`streaming_consumer.py`](streaming_consumer.py) | Reads incoming JSON files using `spark.readStream` (`maxFilesPerTrigger=1`) and prints micro-batch records to console in real-time. | Consumer |
| [`streaming_analytics.py`](streaming_analytics.py) | Uses `foreachBatch` to compute per-batch operational metrics (overall volume/revenue/AOV, restaurant distribution, order status breakdown). | Batch Analytics |
| [`save_streaming_analytics.py`](save_streaming_analytics.py) | Consolidates streaming micro-batches into Snappy Parquet storage (`reports/streaming/processed_orders/`) and exports 3 executive CSV reports. | Consolidation & Export |

---

## 2. Environment & Prerequisites

- **Environment:** Ubuntu 24.04.5 LTS on WSL2 (`/mnt/c/BDA-miniproject`)
- **Python Virtualenv:** `~/venvs/foodieanalytics-spark`
- **Engine:** Apache Spark / PySpark 4.0.1
- **Activation:** `source ~/venvs/foodieanalytics-spark/bin/activate`

---

## 3. Execution Procedure

```bash
# Terminal 1 - Producer
source ~/venvs/foodieanalytics-spark/bin/activate
cd /mnt/c/BDA-miniproject
python3 scripts/streaming/order_generator.py

# Terminal 2 - Streaming Consumer (Monitor stream)
source ~/venvs/foodieanalytics-spark/bin/activate
cd /mnt/c/BDA-miniproject
python3 scripts/streaming/streaming_consumer.py

# Per-Batch Analytics Execution
python3 scripts/streaming/streaming_analytics.py

# Parquet Consolidation & CSV Report Generation
python3 scripts/streaming/save_streaming_analytics.py
```

---

## 4. Input & Output Artifacts

- **Input Stream Source:** `dataset/streaming/orders_input/` (JSON Lines files)
- **Checkpoints:** `dataset/streaming/checkpoint/`, `analytics_checkpoint/`, `consolidation_checkpoint/`
- **Consolidated Parquet Data:** `reports/streaming/processed_orders/` (10 Snappy part files, 50 orders)
- **Summary Reports (CSV):**
  - `reports/streaming/overall_summary/`: Total orders (50), Total revenue (₹50,282.49), AOV (₹1,005.65)
  - `reports/streaming/restaurant_summary/`: Volume and revenue across REST001–REST010
  - `reports/streaming/status_summary/`: Fulfillment stages (Delivered, Out for Delivery, Placed, Preparing)

