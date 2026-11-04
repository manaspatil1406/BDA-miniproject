# Job 3 – Cuisine Distribution Across Bangalore

## Objective

Count the **total number of restaurant menu entries per cuisine type** across all Bangalore localities using Hadoop Streaming and Python 3.

This job answers the question:
> _"Which cuisines are most popular on Bangalore's restaurant scene?"_

---

## Dataset Column Reference

| Column Index | Column Name | Description                                          |
|:------------:|-------------|------------------------------------------------------|
| 11           | `cuisines`  | Comma-separated list of cuisine types per restaurant |

Input file: `hadoop_safe_zomato.csv` (51,656 records, 17 columns, single line per record)

---

## MapReduce Design

### Mapper (`mapper.py`)

- Reads CSV records from standard input using Python's `csv` module.
- Skips the header row on first iteration.
- Extracts `cuisines` (column 11).
- Splits the cuisines field on the comma character (`,`) to get individual cuisine tokens.
- Strips whitespace from each token; skips empty tokens.
- Emits one key-value pair per individual cuisine type:

```
<cuisine>	1
```

**Example input record:**
```
..., "North Indian, Chinese, Biryani", ...
```

**Example mapper output (3 lines for this record):**
```
North Indian	1
Chinese	1
Biryani	1
```

### Shuffle & Sort (Hadoop built-in)

Hadoop collects all mapper `(cuisine, 1)` pairs, sorts them alphabetically by cuisine name, and groups identical cuisines together before routing to the reducer.

### Reducer (`reducer.py`)

- Reads sorted key-value pairs from standard input.
- Accumulates a running `total_count` for each cuisine.
- When the cuisine name changes, emits the final count for the previous cuisine.
- Emits one record per cuisine:

```
<cuisine>	<total_count>
```

**Example output:**
```
Biryani	6483
North Indian	21067
Chinese	15533
```

---

## Execution

### Prerequisites

- Hadoop daemons running (`jps` should show NameNode, DataNode, SecondaryNameNode)
- Dataset uploaded to HDFS: `/foodieanalytics/data/hadoop_safe_zomato.csv`
- `HADOOP_HOME` environment variable set

### Run the Job

```bash
cd scripts/mapreduce/job3_cuisine_distribution
bash run_job.sh
```

### Manual Hadoop Streaming Command

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar \
    -input /foodieanalytics/data/hadoop_safe_zomato.csv \
    -output /foodieanalytics/output/job3_cuisine_distribution \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file mapper.py \
    -file reducer.py
```

### View Results from HDFS

```bash
# List output files
hdfs dfs -ls /foodieanalytics/output/job3_cuisine_distribution/

# View results
hdfs dfs -cat /foodieanalytics/output/job3_cuisine_distribution/part-00000

# Save results locally
hdfs dfs -get /foodieanalytics/output/job3_cuisine_distribution/part-00000 job3_results.txt
```

---

## Output Format

Each output line has two tab-separated fields:

```
<cuisine>	<total_count>
```

| Field         | Type   | Description                                             |
|---------------|--------|---------------------------------------------------------|
| `cuisine`     | string | Cuisine type name                                       |
| `total_count` | int    | Number of restaurant-menu entries featuring this cuisine|

> **Note:** One restaurant may offer multiple cuisines. The count reflects how many restaurants list that cuisine, not unique restaurants.

---

## Actual Results Summary

- **Unique cuisine types identified**: 108
- **Total cuisine-restaurant mappings**: 194,000+ (one restaurant contributes multiple entries)

### Top 15 Cuisines by Frequency

| Rank | Cuisine       | Restaurant Count |
|:----:|---------------|:----------------:|
| 1    | North Indian  | 21,067           |
| 2    | Chinese       | 15,533           |
| 3    | South Indian  | 8,638            |
| 4    | Fast Food     | 8,087            |
| 5    | Biryani       | 6,483            |
| 6    | Continental   | 5,758            |
| 7    | Desserts      | 5,630            |
| 8    | Cafe          | 5,300            |
| 9    | Beverages     | 4,743            |
| 10   | Italian       | 3,386            |
| 11   | Bakery        | 2,838            |
| 12   | Street Food   | 2,593            |
| 13   | Pizza         | 2,072            |
| 14   | Burger        | 2,007            |
| 15   | Seafood       | 1,810            |

### Key Insight

**North Indian and Chinese cuisines dominate** Bangalore's restaurant landscape, together accounting for over 37,000 entries — nearly double the third-place South Indian (8,638). This reflects Bangalore's cosmopolitan, tech-hub character with a large population from northern India.

---

## File Structure

```
job3_cuisine_distribution/
├── mapper.py      — Splits cuisines field, emits (cuisine, 1) per type
├── reducer.py     — Sums counts per cuisine
├── run_job.sh     — Hadoop Streaming job runner script
└── README.md      — This file
```

---

## Notes

- Multi-valued cuisine fields like `"North Indian, Chinese"` are split by comma, making one restaurant contribute to multiple cuisine counts.
- Leading/trailing whitespace around each cuisine token is stripped before emit.
- The output is sorted alphabetically by cuisine name (Hadoop default sort order).
- Result file saved at: `reports/mapreduce_results/job3_cuisine_distribution.txt`

