# Stage 9 – Real-Time Order Stream Processing with Apache Spark Structured Streaming

**Project:** FoodieAnalytics: Food Delivery Analytics and Restaurant Recommendation System using Big Data  
**Stage:** 9 – Simulated Live Order Streaming, Micro-Batch Processing, and Near Real-Time Analytics  
**Technologies:** Apache Spark Structured Streaming, PySpark 4.0.1, Python 3.12, Parquet, JSON, CSV  
**Execution Environment:** Ubuntu 24.04.5 LTS on WSL2 (`/mnt/c/BDA-miniproject`)  

---

## 1. Introduction to Apache Spark Structured Streaming

Apache Spark Structured Streaming is a scalable and fault-tolerant stream processing engine built upon the Spark SQL Catalyst engine. It allows developers to express streaming computations using the same high-level DataFrame and Dataset APIs used for static batch data.

In Structured Streaming, a live data stream is modeled conceptually as an **unbounded table** that continuously appends new records over time. Queries continuously process arriving micro-batches, optimize execution plans through the Catalyst optimizer, and maintain transactional end-to-end fault tolerance with write-ahead logs and state checkpointing.

```
Incoming Stream (JSON files)  ──>  [ Unbounded Input Table ]  ──>  Incremental Query Execution  ──>  [ Result Table / Parquet / Console ]
```

---

## 2. Objective of Stage 9

In real-world food delivery platforms like Zomato or Swiggy, order volumes fluctuate continuously, requiring real-time tracking of operational metrics such as gross merchandise value (GMV), active restaurant order loads, and fulfillment statuses.

The primary objectives of Stage 9 in **FoodieAnalytics** are:
1. **Simulate Live Food Orders:** Generate realistic, synthetic food delivery order streams matching Bangalore platform patterns.
2. **Implement File-Based Streaming Ingestion:** Continuously monitor an ingestion directory for arriving micro-batches using `spark.readStream`.
3. **Execute Real-Time Micro-Batch Analytics:** Compute real-time aggregations (order count, revenue, average order value, order status distributions) per arriving batch using `foreachBatch`.
4. **Persist Consolidated Streaming Data:** Store processed orders into an append-only, columnar **Snappy Parquet** dataset.
5. **Generate Real-Time Executive Reports:** Produce consolidated CSV summary reports for overall performance, restaurant performance, and order status fulfillment.
6. **Ensure Fault Tolerance:** Leverage Spark checkpointing (`checkpointLocation`) to guarantee exactly-once processing semantics without data loss.

---

## 3. Technologies and Tools Used

- **Operating System:** Ubuntu 24.04.5 LTS on Windows Subsystem for Linux (WSL2)
- **Programming Language:** Python 3.12
- **Stream Processing Framework:** Apache Spark Structured Streaming (PySpark 4.0.1)
- **Storage & Ingestion Format:** JSON Lines (simulated live stream source)
- **Analytical Storage:** Columnar Apache Parquet (Snappy compressed)
- **Report Formats:** Comma-Separated Values (CSV) with schema headers
- **Fault-Tolerance Mechanism:** Spark Checkpointing metadata (`commits`, `offsets`, `sources`)

> [!NOTE]
> This stage implements a native **file-based streaming source** where Spark Structured Streaming monitors a directory for incrementally arriving JSON batch files. An external message broker like Apache Kafka was not used.

---

## 4. Simulated Order Data Generation

To simulate live food orders without relying on external network dependencies, an order generator script generates batches of orders and writes them to the streaming input directory at regular intervals.

Each simulated order adheres to the following record schema:

| Field Name | Data Type | Description | Sample Value |
| :--- | :--- | :--- | :--- |
| `order_id` | String | Unique order tracking identifier | `"ORD1001"` |
| `restaurant_id` | String | Target restaurant identifier (10 partner restaurants) | `"REST003"` |
| `customer_id` | String | Customer account identifier | `"CUST89"` |
| `order_amount` | Double | Order financial value in INR ($₹150$ to $₹2,000$) | `452.55` |
| `order_status` | String | Order fulfillment stage (`Placed`, `Preparing`, `Out for Delivery`, `Delivered`) | `"Placed"` |
| `order_timestamp`| String | ISO 8601 creation timestamp | `"2026-11-04T13:15:42.431670"` |

---

## 5. Order Generator Implementation

The script [`scripts/streaming/order_generator.py`](../../scripts/streaming/order_generator.py) generates synthetic order batches:

- **Target Directory:** `/mnt/c/BDA-miniproject/dataset/streaming/orders_input`
- **Total Batches:** 10 micro-batches (`orders_batch_001.json` to `orders_batch_010.json`)
- **Batch Volume:** 5 orders per file (**50 total synthetic orders**)
- **Interval:** 5-second simulated delay between batch writes
- **Restaurant Pool:** 10 synthetic restaurant partners (`REST001` through `REST010`)
- **Order Status Pool:** `Placed`, `Preparing`, `Out for Delivery`, `Delivered`

Each batch is written as newline-delimited JSON (JSON Lines), mimicking the continuous arrival of food delivery events.

---

## 6. Spark Structured Streaming Consumer

The live streaming ingestion consumer is implemented in [`scripts/streaming/streaming_consumer.py`](../../scripts/streaming/streaming_consumer.py):

### Key Consumer Characteristics
1. **Schema Definition:** Explicit `StructType` schema prevents runtime schema inference overhead during stream initialization.
2. **File Stream Configuration:**
   ```python
   orders_stream = (
       spark.readStream
       .format("json")
       .schema(order_schema)
       .option("maxFilesPerTrigger", 1)
       .load(input_path)
   )
   ```
   Setting `maxFilesPerTrigger = 1` forces Spark to process exactly one file per micro-batch trigger, ensuring sequential arrival processing.
3. **Trigger Interval:** `processingTime = "5 seconds"`, coordinating with the generator's arrival cadence.
4. **Output Sink:** Console sink with `outputMode("append")` to print arriving order payloads in real-time.
5. **Checkpointing:** Dedicated checkpoint path (`dataset/streaming/checkpoint`) maintains state offsets.

---

## 7. Execution of the Live Streaming Pipeline

The live streaming pipeline operates across two concurrent terminal sessions:

```
Terminal 1 (Producer): order_generator.py
   Generates orders_batch_001.json  ──(5s delay)──>  orders_batch_002.json  ──> ... ──> orders_batch_010.json
                                        │
                                        ▼ (dataset/streaming/orders_input/)
Terminal 2 (Consumer): streaming_consumer.py
   Trigger Micro-batch 0  ──> Trigger Micro-batch 1  ──> ... ──> Trigger Micro-batch 9
   (Inspects schema, validates offset, prints formatted records to console)
```

---

## 8. Micro-Batch Processing

Spark Structured Streaming decomposes continuous streams into discrete micro-batches. When a new JSON file is placed into `orders_input/`:
1. Spark detects the new file by comparing file metadata against the checkpoint log.
2. An incremental DataFrame is materialized for that micro-batch.
3. The query plan executes transformations specifically over the delta records.
4. Checkpoint commit logs (`commits/`, `offsets/`) are updated atomically.

In Stage 9, **10 discrete micro-batches** (numbered 0 through 9) were ingested and processed, each containing 5 orders.

---

## 9. Real-Time Order Analytics

The script [`scripts/streaming/streaming_analytics.py`](../../scripts/streaming/streaming_analytics.py) demonstrates per-batch statistical evaluation using `foreachBatch`:

```python
def process_batch(batch_df, batch_id):
    batch_df.cache()
    # A. Overall order count, total revenue, average order value
    # B. Group-by restaurant_id (order counts and revenue)
    # C. Group-by order_status (counts per fulfillment stage)
    batch_df.unpersist()
```

By applying `trigger(availableNow=True)`, the analytics script processes all unconsumed batches sequentially until all available data has been analyzed, making it suitable for both continuous daemons and catch-up pipeline runs.

---

## 10. Consolidated Analytics and Parquet Storage

The consolidation pipeline is implemented in [`scripts/streaming/save_streaming_analytics.py`](../../scripts/streaming/save_streaming_analytics.py):

1. **Parquet Ingestion Sink:** In `save_batch(batch_df, batch_id)`, each micro-batch is written in append mode to:
   `reports/streaming/processed_orders/`
   This produces a unified, Snappy-compressed Parquet store containing all 50 processed orders across 10 part files.
2. **Consolidation Read:** After streaming ingestion terminates, Spark reads the consolidated Parquet store back into a standard DataFrame (`orders_df = spark.read.parquet(processed_path)`).
3. **Executive Reporting:** Spark SQL aggregations compute three summary reports and export them as single-partition CSV files (`.coalesce(1).write.csv(...)`).

---

## 11. Overall Order Summary

From [`reports/streaming/overall_summary/part-00000-...csv`](../../reports/streaming/overall_summary/):

| Metric | Verified Value | Interpretation |
| :--- | :---: | :--- |
| **Total Orders Ingested** | **50** | All 10 micro-batches of 5 orders ingested without loss. |
| **Total Gross Revenue** | **₹50,282.49** | Sum of financial order amounts across all restaurants. |
| **Average Order Value (AOV)** | **₹1,005.65** | Mean transaction size across the 50 simulated orders. |

---

## 12. Restaurant-Wise Analytics

From [`reports/streaming/restaurant_summary/part-00000-...csv`](../../reports/streaming/restaurant_summary/):

| Restaurant ID | Orders Received | Total Revenue (INR) | Average Order Value (INR) |
| :---: | :---: | :---: | :---: |
| **REST004** | 9 | ₹10,871.00 | ₹1,207.89 |
| **REST003** | 9 | ₹9,575.16 | ₹1,063.91 |
| **REST010** | 7 | ₹6,709.80 | ₹958.54 |
| **REST008** | 6 | ₹6,912.00 | ₹1,152.00 |
| **REST006** | 5 | ₹4,350.23 | ₹870.05 |
| **REST007** | 4 | ₹2,695.81 | ₹673.95 |
| **REST001** | 4 | ₹2,660.33 | ₹665.08 |
| **REST005** | 3 | ₹2,151.81 | ₹717.27 |
| **REST002** | 2 | ₹2,371.60 | ₹1,185.80 |
| **REST009** | 1 | ₹1,984.75 | ₹1,984.75 |
| **Total** | **50** | **₹50,282.49** | **₹1,005.65** |

### Key Observations
- **REST004** and **REST003** handled the highest order volume (9 orders each), accounting for **₹20,446.16** (40.66% of total platform revenue).
- **REST009** logged the highest Average Order Value (**₹1,984.75**), representing a premium group dining order.
- All 10 simulated partner restaurants registered active orders, validating multi-tenant ingestion.

---

## 13. Order Status Distribution

From [`reports/streaming/status_summary/part-00000-...csv`](../../reports/streaming/status_summary/):

| Order Status | Order Count | Total Order Value (INR) | Share of Orders |
| :--- | :---: | :---: | :---: |
| **Delivered** | 17 | ₹15,967.79 | 34.0% |
| **Out for Delivery** | 16 | ₹16,187.23 | 32.0% |
| **Placed** | 9 | ₹11,181.44 | 18.0% |
| **Preparing** | 8 | ₹6,946.03 | 16.0% |
| **Total** | **50** | **₹50,282.49** | **100.0%** |

### Fulfillment Insights
- **66.0%** of orders (33 of 50) have advanced into or completed the delivery phase (`Out for Delivery` or `Delivered`).
- **34.0%** of orders (17 of 50) are in the early kitchen lifecycle (`Placed` or `Preparing`).
- Financial value corresponds closely with stage proportions, confirming consistent distribution across order sizes.

---

## 14. Checkpointing and Fault Tolerance

Fault tolerance is critical for distributed stream processing. Spark Structured Streaming implements transactional checkpointing through three directories:

```text
dataset/streaming/
├── checkpoint/                   # For streaming_consumer.py console queries
│   ├── commits/                  # Atomic commit completion logs
│   ├── offsets/                  # Source file offset tracking
│   └── sources/                  # Metadata regarding discovered input files
├── analytics_checkpoint/         # For streaming_analytics.py foreachBatch
└── consolidation_checkpoint/     # For save_streaming_analytics.py Parquet export
```

### Fault-Tolerance Principles Applied
- **Exactly-Once Processing:** Offsets record the exact JSON files processed in each micro-batch. If a worker fails, Spark replays the uncommitted batch using recorded offsets.
- **Idempotent Storage:** Parquet write operations commit synchronously with checkpoint log updates, preventing duplicate data ingestion.
- **State Recovery:** Checkpoint metadata persists across script restarts, allowing pipeline components to resume without reprocessing previously consumed files.

---

## 15. Outputs and Generated Artifacts

```text
reports/streaming/
│
├── processed_orders/                   # Consolidated Parquet dataset (10 Snappy part files)
│   ├── part-00000-...snappy.parquet    # Micro-batch 0 records
│   ├── part-00000-...snappy.parquet    # Micro-batch 1 records
│   ├── ...                             # Micro-batches 2 through 8
│   ├── part-00000-...snappy.parquet    # Micro-batch 9 records
│   └── _SUCCESS                        # Spark success marker
│
├── overall_summary/                    # Executive KPI CSV report
│   ├── part-00000-...csv               # total_orders=50, revenue=50282.49, AOV=1005.65
│   └── _SUCCESS
│
├── restaurant_summary/                 # Per-restaurant breakdown CSV report
│   ├── part-00000-...csv               # 10 partner restaurants ranked by volume
│   └── _SUCCESS
│
└── status_summary/                     # Order lifecycle fulfillment CSV report
    ├── part-00000-...csv               # Placed, Preparing, Out for Delivery, Delivered
    └── _SUCCESS
```

---

## 16. Advantages of Spark Structured Streaming

1. **Unified Batch and Stream API:** The exact same SQL transformations (`count`, `sum`, `avg`, `groupBy`) run seamlessly on live streaming DataFrames and historical Parquet tables.
2. **End-to-End Exactly-Once Guarantees:** Through write-ahead logs and checkpointing, duplicate reads and data omissions are prevented.
3. **Catalyst Optimization:** Continuous streaming queries benefit from Spark's cost-based query optimization, predicate pushdown, and Tungsten binary memory management.
4. **Columnar Parquet Interoperability:** Micro-batches append directly into standard Parquet tables, making live operational data queryable by downstream batch systems (e.g. Hive, Spark SQL, MLlib) without complex ETL pipelines.
5. **Backpressure and Trigger Throttling:** `maxFilesPerTrigger` controls input rates, preventing worker cluster memory saturation during peak order traffic.

---

## 17. Execution Procedure

To reproduce the Stage 9 streaming pipeline inside **Ubuntu WSL2**:

### Step 1: Activate the Python Virtual Environment
```bash
source ~/venvs/foodieanalytics-spark/bin/activate
cd /mnt/c/BDA-miniproject
```

### Step 2: Launch the Order Generator
In Terminal 1:
```bash
python3 scripts/streaming/order_generator.py
```
*Generates 10 batches of 5 orders into `dataset/streaming/orders_input/` with 5-second pauses.*

### Step 3: Run the Streaming Consumer (Live Monitoring)
In Terminal 2 (concurrent with Step 2 or immediately after):
```bash
python3 scripts/streaming/streaming_consumer.py
```
*Displays incoming order micro-batches to the console in real-time.*

### Step 4: Run Real-Time Batch Analytics
```bash
python3 scripts/streaming/streaming_analytics.py
```
*Processes available JSON batches and displays per-batch operational metrics.*

### Step 5: Execute Consolidated Analytics and Storage
```bash
python3 scripts/streaming/save_streaming_analytics.py
```
*Appends micro-batches into consolidated Parquet storage and writes the three final CSV reports.*

### Step 6: Verify Generated Artifacts
```bash
# Check consolidated Parquet part files
ls -l reports/streaming/processed_orders/

# View overall summary CSV
cat reports/streaming/overall_summary/*.csv

# View restaurant summary CSV
cat reports/streaming/restaurant_summary/*.csv

# View order status summary CSV
cat reports/streaming/status_summary/*.csv
```

---

## 18. Conclusion

Stage 9 extends the **FoodieAnalytics** platform into near real-time operational processing. By combining simulated JSON live order streaming, Apache Spark Structured Streaming micro-batching, transactional checkpointing, and columnar Parquet consolidation, the system demonstrates how modern food delivery aggregators handle real-time inventory tracking, partner revenue accounting, and order fulfillment visibility at scale.

---

*Report prepared for FoodieAnalytics — Big Data Analytics Mini Project | Stage 9: Spark Structured Streaming*

