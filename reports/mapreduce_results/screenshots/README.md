# MapReduce Job Execution Screenshots

This directory should contain screenshot evidence of the Hadoop MapReduce job executions performed in Ubuntu WSL2.

---

## Screenshot Checklist

Capture the following screenshots to document Stage 4 execution. Name each file as suggested.

### 1. Hadoop Daemon Status

| File Name              | Content                                           |
|------------------------|---------------------------------------------------|
| `01_jps_daemons.png`   | Output of `jps` showing all 3 running daemons: NameNode, DataNode, SecondaryNameNode |

**Command to run:**
```bash
jps
```

---

### 2. HDFS Input Dataset

| File Name              | Content                                           |
|------------------------|---------------------------------------------------|
| `02_hdfs_input.png`    | `hdfs dfs -ls /foodieanalytics/data/` showing `hadoop_safe_zomato.csv` |

**Command to run:**
```bash
hdfs dfs -ls /foodieanalytics/data/
```

---

### 3. Job 1 – Restaurant Count by Locality

| File Name                   | Content                                      |
|-----------------------------|----------------------------------------------|
| `03_job1_execution.png`     | Terminal output of running `job1` via `bash run_job.sh` |
| `04_job1_output_preview.png`| `hdfs dfs -cat` showing first 10 lines of job1 output |

**Commands to run:**
```bash
cd scripts/mapreduce/job1_restaurant_count
bash run_job.sh

hdfs dfs -cat /foodieanalytics/output/job1_restaurant_count/part-00000 | head -20
```

---

### 4. Job 2 – Average Rating by Locality

| File Name                   | Content                                      |
|-----------------------------|----------------------------------------------|
| `05_job2_execution.png`     | Terminal output of running job2              |
| `06_job2_output_preview.png`| First 10 lines of job2 HDFS output          |

**Commands to run:**
```bash
cd scripts/mapreduce/job2_average_rating
bash run_job.sh

hdfs dfs -cat /foodieanalytics/output/job2_average_rating/part-00000 | head -20
```

---

### 5. Job 3 – Cuisine Distribution

| File Name                   | Content                                      |
|-----------------------------|----------------------------------------------|
| `07_job3_execution.png`     | Terminal output of running job3              |
| `08_job3_output_preview.png`| First 10 lines of job3 HDFS output          |

**Commands to run:**
```bash
cd scripts/mapreduce/job3_cuisine_distribution
bash run_job.sh

hdfs dfs -cat /foodieanalytics/output/job3_cuisine_distribution/part-00000 | head -20
```

---

### 6. Job 4 – Online Ordering Analysis

| File Name                   | Content                                      |
|-----------------------------|----------------------------------------------|
| `09_job4_execution.png`     | Terminal output of running job4              |
| `10_job4_output_preview.png`| First 10 lines of job4 HDFS output          |

**Commands to run:**
```bash
cd scripts/mapreduce/job4_online_order_analysis
bash run_job.sh

hdfs dfs -cat /foodieanalytics/output/job4_online_order_analysis/part-00000 | head -20
```

---

### 7. HDFS Output Directory Listing

| File Name                   | Content                                      |
|-----------------------------|----------------------------------------------|
| `11_hdfs_all_outputs.png`   | `hdfs dfs -ls /foodieanalytics/output/` showing all 4 output directories |

**Command to run:**
```bash
hdfs dfs -ls /foodieanalytics/output/
```

---

### 8. NameNode Web UI (Optional)

| File Name                | Content                                         |
|--------------------------|-------------------------------------------------|
| `12_namenode_webui.png`  | Browser screenshot of NameNode UI at `http://localhost:9870` showing HDFS health |

---

## Instructions

1. Start Hadoop daemons in Ubuntu WSL2:
   ```bash
   start-dfs.sh
   ```
2. Open a terminal in Ubuntu WSL2.
3. Run each command listed above.
4. Use a screenshot tool (Windows `Win+Shift+S`, or `scrot` in WSL2) to capture the output.
5. Save each screenshot in this directory with the suggested filename.

> These screenshots are required for your project viva/presentation as evidence that the MapReduce jobs were actually executed on a real Hadoop cluster (single-node mode).

