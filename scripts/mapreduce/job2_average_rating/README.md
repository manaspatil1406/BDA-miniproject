# Job 2 – Average Restaurant Rating by Locality

## Objective

Calculate the **average star rating** of restaurants in each Bangalore locality using Hadoop Streaming and Python 3.

This job answers the question:
> _"Which areas of Bangalore have the highest and lowest restaurant quality?"_

---

## Dataset Column Reference

| Column Index | Column Name    | Description                        |
|:------------:|----------------|------------------------------------|
| 8            | `location`     | Locality / area name               |
| 5            | `rate`         | Restaurant rating (float, 1.0–5.0) |

Input file: `hadoop_safe_zomato.csv` (51,656 records, 17 columns, single line per record)

---

## MapReduce Design

### Mapper (`mapper.py`)

- Reads CSV records from standard input using Python's `csv` module.
- Skips the header row on first iteration.
- Extracts `location` (column 8) and `rate` (column 5).
- Skips rows where either field is empty, or where the rating cannot be cast to `float`.
- Validates rating is within the 1–5 range; discards out-of-range values.
- Emits one key-value pair per valid record:

```
<location>\t<rating>
```

**Example output:**
```
BTM	3.8
HSR	4.2
Koramangala 5th Block	4.0
```

### Shuffle & Sort (Hadoop built-in)

Hadoop sorts all mapper output by key (location) alphabetically, grouping all rating values for the same locality together before routing to the reducer.

### Reducer (`reducer.py`)

- Reads sorted key-value pairs from standard input.
- Accumulates `total_rating` and `rating_count` for each locality.
- When the locality changes, computes `average = total_rating / rating_count`.
- Emits one record per locality:

```
<location>\t<average_rating>\t<rated_count>
```

**Example output:**
```
BTM	3.57	3924
Church Street	3.99	545
Bommanahalli	3.19	144
```

---

## Execution

### Prerequisites

- Hadoop daemons running (`jps` should show NameNode, DataNode, SecondaryNameNode)
- Dataset uploaded to HDFS: `/foodieanalytics/data/hadoop_safe_zomato.csv`
- `HADOOP_HOME` environment variable set

### Run the Job

```bash
cd scripts/mapreduce/job2_average_rating
bash run_job.sh
```

### Manual Hadoop Streaming Command

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar \
    -input /foodieanalytics/data/hadoop_safe_zomato.csv \
    -output /foodieanalytics/output/job2_average_rating \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file mapper.py \
    -file reducer.py
```

### View Results from HDFS

```bash
# List output files
hdfs dfs -ls /foodieanalytics/output/job2_average_rating/

# View results
hdfs dfs -cat /foodieanalytics/output/job2_average_rating/part-00000

# Save results locally
hdfs dfs -get /foodieanalytics/output/job2_average_rating/part-00000 job2_results.txt
```

---

## Output Format

Each output line has three tab-separated fields:

```
<location>	<average_rating>	<rated_count>
```

| Field           | Type   | Description                                    |
|-----------------|--------|------------------------------------------------|
| `location`      | string | Locality name                                  |
| `average_rating`| float  | Mean rating rounded to 2 decimal places        |
| `rated_count`   | int    | Number of restaurants with a valid rating      |

---

## Actual Results Summary

- **Localities analysed**: 92 (out of 93 total; 1 locality had no valid ratings)
- **Total rated restaurants**: 48,226 (remaining records had missing/invalid ratings)

### Top-Rated Localities

| Locality              | Avg Rating | Rated Count |
|-----------------------|:----------:|:-----------:|
| Church Street         | 3.99       | 545         |
| Cunningham Road       | 3.90       | 475         |
| MG Road               | 3.87       | 1,047       |
| Lavelle Road          | 3.85       | 274         |
| Brigade Road          | 3.71       | 1,052       |

### Lowest-Rated Localities

| Locality              | Avg Rating | Rated Count |
|-----------------------|:----------:|:-----------:|
| Bommanahalli          | 3.19       | 144         |
| CV Raman Nagar        | 3.48       | 60          |
| Banaswadi             | 3.50       | 480         |

### High-Volume Localities

| Locality              | Avg Rating | Rated Count |
|-----------------------|:----------:|:-----------:|
| BTM                   | 3.57       | 3,924       |
| HSR                   | 3.67       | 2,019       |
| Bannerghatta Road     | 3.51       | 1,235       |

---

## File Structure

```
job2_average_rating/
├── mapper.py      — Extracts location + rating, emits key-value pairs
├── reducer.py     — Aggregates ratings, computes average per locality
├── run_job.sh     — Hadoop Streaming job runner script
└── README.md      — This file
```

---

## Notes

- Ratings stored as strings like `"3.8"` in the CSV are cast to `float`; non-numeric values (e.g. `"NEW"`, `"-"`) are skipped.
- Only ratings in range `[1.0, 5.0]` are accepted; others are discarded.
- The output is sorted alphabetically by locality name (Hadoop default sort order).
- Result file saved at: `reports/mapreduce_results/job2_average_rating.txt`

