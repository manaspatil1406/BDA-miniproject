# MapReduce Results – FoodieAnalytics Stage 4

This directory contains the result files retrieved from HDFS after running all four Hadoop MapReduce jobs on the Zomato Bangalore restaurant dataset.

---

## Environment

| Component      | Details                                              |
|----------------|------------------------------------------------------|
| OS             | Ubuntu 24.04.5 LTS on WSL2                          |
| Java           | OpenJDK 21                                          |
| Hadoop         | 3.4.3                                               |
| Execution Mode | Hadoop Streaming with Python 3                      |
| HDFS NameNode  | `hdfs://localhost:9000`                             |
| Input Dataset  | `/foodieanalytics/data/hadoop_safe_zomato.csv`      |
| Records        | 51,656 restaurants, 17 columns                      |

---

## Result Files

### `job1_restaurant_count.txt`

**HDFS source:** `/foodieanalytics/output/job1_restaurant_count/part-00000`

**Format:**
```
<location>	<restaurant_count>
```

**Summary:**
- 93 unique localities
- 51,656 total restaurants

**Top localities by restaurant count:**

| Locality          | Count  |
|-------------------|:------:|
| BTM               | 5,114  |
| HSR               | 2,522  |
| Bannerghatta Road | 1,630  |
| Bellandur         | 1,286  |
| Brigade Road      | 1,216  |

---

### `job2_average_rating.txt`

**HDFS source:** `/foodieanalytics/output/job2_average_rating/part-00000`

**Format:**
```
<location>	<avg_rating>	<rated_count>
```

**Summary:**
- 92 localities have at least one rated restaurant
- Ratings range from 3.19 (Bommanahalli) to 3.99 (Church Street)

**Highest-rated localities:**

| Locality         | Avg Rating | Rated Count |
|------------------|:----------:|:-----------:|
| Church Street    | 3.99       | 545         |
| Cunningham Road  | 3.90       | 475         |
| MG Road          | 3.87       | 1,047       |
| Lavelle Road     | 3.85       | 274         |
| Brigade Road     | 3.71       | 1,052       |

---

### `job3_cuisine_distribution.txt`

**HDFS source:** `/foodieanalytics/output/job3_cuisine_distribution/part-00000`

**Format:**
```
<cuisine>	<restaurant_count>
```

**Summary:**
- 108 unique cuisine types identified
- North Indian (21,067) and Chinese (15,533) are the dominant cuisines

**Top 10 cuisines:**

| Rank | Cuisine      | Count  |
|:----:|--------------|:------:|
| 1    | North Indian | 21,067 |
| 2    | Chinese      | 15,533 |
| 3    | South Indian | 8,638  |
| 4    | Fast Food    | 8,087  |
| 5    | Biryani      | 6,483  |
| 6    | Continental  | 5,758  |
| 7    | Desserts     | 5,630  |
| 8    | Cafe         | 5,300  |
| 9    | Beverages    | 4,743  |
| 10   | Italian      | 3,386  |

---

### `job4_online_order_analysis.txt`

**HDFS source:** `/foodieanalytics/output/job4_online_order_analysis/part-00000`

**Format:**
```
<location>	<online_count>	<total_count>	<adoption_pct>
```

**Summary:**
- 93 localities analysed
- City-wide online ordering adoption: ~62% of all restaurants
- HSR Layout leads at 76.13%; City Market lowest at 22.22%

**Top adoption localities:**

| Locality       | Online | Total | Adoption % |
|----------------|:------:|:-----:|:----------:|
| HSR            | 1,920  | 2,522 | 76.13%     |
| HBR Layout     | 105    | 153   | 68.63%     |
| BTM            | 3,325  | 5,114 | 65.02%     |
| Basavanagudi   | 441    | 684   | 64.47%     |
| CV Raman Nagar | 56     | 89    | 62.92%     |

---

## Screenshots

The `screenshots/` directory is the designated location for visual evidence of job execution.

See [`screenshots/README.md`](screenshots/README.md) for the full checklist of screenshots to capture.

---

## How to Reproduce

To retrieve fresh results from HDFS:

```bash
# Job 1
hdfs dfs -get /foodieanalytics/output/job1_restaurant_count/part-00000 job1_restaurant_count.txt

# Job 2
hdfs dfs -get /foodieanalytics/output/job2_average_rating/part-00000 job2_average_rating.txt

# Job 3
hdfs dfs -get /foodieanalytics/output/job3_cuisine_distribution/part-00000 job3_cuisine_distribution.txt

# Job 4
hdfs dfs -get /foodieanalytics/output/job4_online_order_analysis/part-00000 job4_online_order_analysis.txt
```

> **Note:** These files are the exact, unmodified output from HDFS. Do not edit them manually.

