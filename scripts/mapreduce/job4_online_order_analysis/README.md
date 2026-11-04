# Job 4 – Online Ordering Adoption by Locality

## Objective

Calculate the **online ordering adoption rate** (percentage of restaurants accepting online orders) for each Bangalore locality using Hadoop Streaming and Python 3.

This job answers the question:
> _"Which areas of Bangalore have the highest digital adoption for food ordering?"_

---

## Dataset Column Reference

| Column Index | Column Name    | Description                                     |
|:------------:|----------------|-------------------------------------------------|
| 8            | `location`     | Locality / area name                            |
| 3            | `online_order` | Whether the restaurant accepts online orders    |

The `online_order` column contains `True` or `False` string values (case-insensitive).

Input file: `hadoop_safe_zomato.csv` (51,656 records, 17 columns, single line per record)

---

## MapReduce Design

### Mapper (`mapper.py`)

- Reads CSV records from standard input using Python's `csv` module.
- Skips the header row on first iteration.
- Extracts `location` (column 8) and `online_order` (column 3).
- Normalises `online_order` to lowercase; skips any record where the value is not `"true"` or `"false"`.
- If location is empty, assigns the placeholder `"Unknown"`.
- Emits one key-value pair per record:

```
<location>	<online_flag>	1
```

Where `online_flag` is `1` if the restaurant accepts online orders, `0` otherwise.

**Example output:**
```
BTM	1	1
HSR	0	1
Koramangala 5th Block	1	1
```

### Shuffle & Sort (Hadoop built-in)

Hadoop sorts all mapper output by key (location) alphabetically, grouping all records for the same locality together before routing to the reducer.

### Reducer (`reducer.py`)

- Reads sorted key-value triples from standard input.
- Accumulates `online_total` (sum of online flags) and `restaurant_total` (sum of record counts) per locality.
- When the locality changes, calculates the adoption percentage:

  `adoption_pct = (online_total / restaurant_total) × 100`

- Emits one record per locality:

```
<location>	<online_count>	<total_count>	<adoption_pct>
```

**Example output:**
```
BTM	3325	5114	65.02
HSR	1920	2522	76.13
City Market	28	126	22.22
```

---

## Execution

### Prerequisites

- Hadoop daemons running (`jps` should show NameNode, DataNode, SecondaryNameNode)
- Dataset uploaded to HDFS: `/foodieanalytics/data/hadoop_safe_zomato.csv`
- `HADOOP_HOME` environment variable set

### Run the Job

```bash
cd scripts/mapreduce/job4_online_order_analysis
bash run_job.sh
```

### Manual Hadoop Streaming Command

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar \
    -input /foodieanalytics/data/hadoop_safe_zomato.csv \
    -output /foodieanalytics/output/job4_online_order_analysis \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file mapper.py \
    -file reducer.py
```

### View Results from HDFS

```bash
# List output files
hdfs dfs -ls /foodieanalytics/output/job4_online_order_analysis/

# View results
hdfs dfs -cat /foodieanalytics/output/job4_online_order_analysis/part-00000

# Save results locally
hdfs dfs -get /foodieanalytics/output/job4_online_order_analysis/part-00000 job4_results.txt
```

---

## Output Format

Each output line has four tab-separated fields:

```
<location>	<online_count>	<total_count>	<adoption_pct>
```

| Field          | Type   | Description                                          |
|----------------|--------|------------------------------------------------------|
| `location`     | string | Locality name                                        |
| `online_count` | int    | Number of restaurants accepting online orders        |
| `total_count`  | int    | Total number of restaurants in this locality         |
| `adoption_pct` | float  | Online ordering adoption rate (%, 2 decimal places)  |

---

## Actual Results Summary

- **Localities analysed**: 93
- **Total restaurants**: 51,656

### Top 5 — Highest Online Ordering Adoption

| Locality              | Online | Total | Adoption % |
|-----------------------|:------:|:-----:|:----------:|
| HSR                   | 1,920  | 2,522 | **76.13%** |
| HBR Layout            | 105    | 153   | **68.63%** |
| BTM                   | 3,325  | 5,114 | **65.02%** |
| Basavanagudi          | 441    | 684   | **64.47%** |
| CV Raman Nagar        | 56     | 89    | **62.92%** |

### Bottom 5 — Lowest Online Ordering Adoption

| Locality              | Online | Total | Adoption % |
|-----------------------|:------:|:-----:|:----------:|
| City Market           | 28     | 126   | **22.22%** |
| Central Bangalore     | 3      | 8     | **37.50%** |
| Commercial Street     | 142    | 370   | **38.38%** |
| Brigade Road          | 663    | 1,216 | **54.52%** |
| Banaswadi             | 343    | 664   | **51.66%** |

### Key Insight

**HSR Layout leads with 76.13% adoption**, reflecting its tech-savvy population. In contrast, **City Market has just 22.22% adoption**, consistent with its traditional market character where in-person dining is the norm.

---

## File Structure

```
job4_online_order_analysis/
├── mapper.py      — Extracts location + online_order flag, emits (location, flag, 1)
├── reducer.py     — Sums online and total counts, computes adoption percentage
├── run_job.sh     — Hadoop Streaming job runner script
└── README.md      — This file
```

---

## Notes

- `online_order` values are lowercased before comparison to handle any casing variations.
- Records where `online_order` is neither `"true"` nor `"false"` are skipped entirely.
- Empty location fields are mapped to `"Unknown"` rather than being discarded.
- The adoption percentage uses the full restaurant count (not just rated ones) as the denominator.
- Result file saved at: `reports/mapreduce_results/job4_online_order_analysis.txt`

