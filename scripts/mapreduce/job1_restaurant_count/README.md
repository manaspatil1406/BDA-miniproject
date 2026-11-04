# MapReduce Job 1: Restaurant Count by Locality
**Project**: FoodieAnalytics (Big Data Analytics Mini Project)  
**Framework**: Apache Hadoop 3.4.3 Streaming with Python 3  
**Cluster Mode**: Single-Node Pseudo-Distributed on Ubuntu 24.04 LTS (WSL2)  

---

## 1. Overview & Objective

The objective of **MapReduce Job 1** is to compute the frequency distribution of restaurants across distinct geographic localities in Bangalore. 

This job implements the classic MapReduce aggregation pattern:
$$\text{Map}(\text{Row}) \rightarrow (\text{Location}, 1) \xrightarrow{\text{Shuffle \& Sort}} \text{Reduce}(\text{Location}, [1, 1, \dots]) \rightarrow (\text{Location}, \sum \text{Count})$$

### Business & Analytical Value
* **Spatial Market Density**: Identifies high-density culinary epicenters (e.g., BTM Layout, Koramangala, Indiranagar).
* **Delivery Supply Optimization**: Helps food delivery platforms determine where partner restaurant coverage is saturated versus underserved.

---

## 2. Directory Structure

```text
scripts/mapreduce/job1_restaurant_count/
├── mapper.py       # Python 3 mapper extracting <location>\t1
├── reducer.py      # Python 3 reducer aggregating counts per locality
├── run_job.sh      # Bash execution script for Hadoop Streaming
└── README.md       # Comprehensive documentation and execution guide
```

---

## 3. Architecture & Code Explanation

### 3.1 Mapper (`mapper.py`)
* **Input**: Streams CSV text lines from standard input (`sys.stdin`) provided by Hadoop's `TextInputFormat`.
* **Field Extraction**: Utilizes Python's `csv.reader` to safely parse fields.
* **Header Guard**: Automatically detects and skips the CSV header row (`url` or `location`).
* **Column Extracted**: Column index 8 (`location`).
* **Null Handling**: If a location value is blank or `'nan'`, it emits `"Unknown"`.
* **Output**: Writes tab-delimited key-value pairs to standard output (`sys.stdout`):
  ```text
  BTM	1
  HSR	1
  Koramangala 5th Block	1
  ```

### 3.2 Hadoop Shuffle & Sort Phase
* The Hadoop framework partitions mapper outputs and sorts all keys alphabetically.
* All occurrences of the same locality arrive sequentially at the reducer.

### 3.3 Reducer (`reducer.py`)
* **Input**: Reads sorted `<location>\t1` pairs from standard input (`sys.stdin`).
* **Aggregation**: Maintains a running accumulator (`current_count`) for the active `current_location`.
* **Key Transition**: When a new locality key is encountered, the reducer emits the accumulated total for the previous locality and resets the counter.
* **Stream Termination**: Emits the final accumulated locality after standard input reaches EOF.
* **Output**: Tab-separated locality and integer count:
  ```text
  BTM	5114
  HSR	2522
  Koramangala 5th Block	2504
  ```

---

## 4. Multiline CSV Handling & Intermediate Dataset

### The Challenge
Hadoop Streaming uses `TextInputFormat` by default, which splits inputs strictly at byte boundaries and newline characters (`\n` or `\r\n`). In `cleaned_zomato.csv`, the free-form `reviews_list` column contains embedded physical newline characters (23,012 extra lines across reviews). If passed directly, Hadoop fractures single restaurant records across multiple mapper splits.

### The Safe Solution
We implemented a non-destructive intermediate dataset:
* **Path**: `dataset/processed/hadoop_safe_zomato.csv`
* **Mechanism**: Replaces physical embedded carriage returns and newlines inside quoted cell text with a single space.
* **Integrity**: Exactly **51,657 physical lines** (1 header + 51,656 data records), guaranteeing that **1 line == 1 complete restaurant record**.
* **Safety**: The original `cleaned_zomato.csv` remains **100% unaltered**.

---

## 5. Execution Guide (From Ubuntu 24.04 WSL2)

### Step 5.1: Local Pipe Simulation (Test Before Cluster Run)
You can verify the mapper and reducer logic locally without starting Hadoop:
```bash
# Test first 100 rows
head -n 100 dataset/processed/hadoop_safe_zomato.csv | python3 scripts/mapreduce/job1_restaurant_count/mapper.py | sort | python3 scripts/mapreduce/job1_restaurant_count/reducer.py
```

### Step 5.2: Run the Hadoop Streaming Job on Cluster
Ensure Hadoop daemons are running (`jps`), then execute:
```bash
bash scripts/mapreduce/job1_restaurant_count/run_job.sh
```

### Manual Command Alternative:
```bash
hadoop jar /home/manas_patil/apps/hadoop-3.4.3/share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar \
  -input /foodieanalytics/data/hadoop_safe_zomato.csv \
  -output /foodieanalytics/output/job1_restaurant_count \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -file scripts/mapreduce/job1_restaurant_count/mapper.py \
  -file scripts/mapreduce/job1_restaurant_count/reducer.py
```

---

## 6. Expected Output Format

The output is written to `/foodieanalytics/output/job1_restaurant_count/part-00000` in HDFS:

```text
BTM	5114
HSR	2522
Koramangala 5th Block	2504
JP Nagar	2234
Whitefield	2142
Indiranagar	2079
Jayanagar	1926
Marathahalli	1843
Bannerghatta Road	1628
Bellandur	1283
...
```

### Inspection Commands:
```bash
# List output files
hdfs dfs -ls /foodieanalytics/output/job1_restaurant_count

# View Top 10 Localities
hdfs dfs -cat /foodieanalytics/output/job1_restaurant_count/part-* | sort -k2,2nr | head -n 10

# Verify Total Aggregated Count (Must equal 51,656)
hdfs dfs -cat /foodieanalytics/output/job1_restaurant_count/part-* | awk '{sum+=$2} END {print sum}'
```

