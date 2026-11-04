# Stage 10 – Interactive Dashboard and Final Application

**Project:** FoodieAnalytics: Food Delivery Analytics and Restaurant Recommendation System using Big Data  
**Stage:** 10 – Full-Stack Streamlit Analytics Dashboard  
**Technologies:** Streamlit 1.66.1, Pandas 3.1.0, Plotly 7.1.0, PyArrow 26.0.1  
**Execution Environment:** Ubuntu 24.04.5 LTS on WSL2 · Python 3.12.3  

---

## 1. Introduction

Stage 10 represents the final deliverable of the FoodieAnalytics Big Data Analytics pipeline. This stage integrates all analytical outputs generated across Stages 1–9 into a single, interactive, multi-page web application built with Streamlit.

The application serves as the **Business Intelligence and Decision-Support** layer of the architecture, providing stakeholders with intuitive access to:

- Macro-level market intelligence about Bangalore's 51,656-restaurant ecosystem.
- Locality-level restaurant concentration, rating, pricing, and delivery analytics.
- Cuisine popularity trends and geographic distribution patterns.
- An ML-powered restaurant recommendation engine derived from Stage 8 K-Means clustering.
- Near real-time streaming order monitoring from Stage 9 Spark Structured Streaming.

---

## 2. Objectives

1. **Integrate** all precomputed Spark SQL, Hive, MLlib, and Streaming outputs into a unified UI layer.
2. **Visualize** key business metrics through interactive Plotly charts, KPI cards, and drill-down tables.
3. **Expose** the Stage 8 weighted recommendation formula as an interactive recommendation engine.
4. **Display** Stage 9 streaming simulation results (50 orders, 10 restaurants, 4 order statuses).
5. **Deliver** production-quality UI with custom Zomato-inspired CSS theming.

---

## 3. Technology Stack

| Technology | Version | Role |
|:-----------|:--------|:-----|
| Streamlit | 1.66.1 | Multi-page application framework |
| Plotly | 7.1.0 | Interactive chart rendering |
| Pandas | 3.1.0 | Tabular data processing and filtering |
| PyArrow | 26.0.1 | High-performance Parquet I/O |
| Python | 3.12.3 | Application runtime |
| Ubuntu WSL2 | 24.04.5 LTS | Execution environment |

> [!NOTE]
> This stage uses **no live Spark Session** during the dashboard runtime. All heavy computations (MapReduce, Hive, Spark SQL, MLlib, Streaming) were executed in Stages 1–9 and their precomputed outputs are served by the dashboard via Pandas + PyArrow for instant rendering with zero JVM startup overhead.

---

## 4. Application Architecture

```
app/
├── Home.py                          # Landing page & executive KPI dashboard
├── pages/
│   ├── 1_Restaurant_Analytics.py   # Rating, votes, correlation, explorer
│   ├── 2_Cuisine_Location_Analysis.py  # Cuisine dominance & locality deep-dive
│   ├── 3_Pricing_Delivery_Analytics.py # Cost tiers & online delivery adoption
│   ├── 4_Restaurant_Recommendation.py  # ML recommendation engine (Stage 8)
│   └── 5_Live_Order_Monitoring.py      # Streaming analytics (Stage 9)
├── utils/
│   ├── __init__.py                 # Package exports
│   ├── data_loader.py              # Cached data retrieval & KPI computation
│   └── recommendation_engine.py   # Pandas-based scoring & ranking logic
└── assets/
    └── styles.css                  # Zomato-inspired red/white UI theme
```

### Data Flow

```
Precomputed Outputs (Stages 1–9)
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│  data_loader.py  (@st.cache_data TTL=3600s)              │
│  ┌──────────────┐  ┌────────────────────────────────┐   │
│  │ Spark Queries│  │ Clustered Restaurants Parquet  │   │
│  │ Q01 – Q08   │  │ (51,656 rows, 10 columns)       │   │
│  │ CSV part files│  │ PyArrow reader, no Spark JVM   │   │
│  └──────────────┘  └────────────────────────────────┘   │
│  ┌──────────────────────────────────┐                    │
│  │ Streaming Summaries (Stage 9)    │                    │
│  │ overall + restaurant + status CSV│                    │
│  │ processed_orders Snappy Parquet  │                    │
│  └──────────────────────────────────┘                    │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
              Streamlit Page Rendering
              (Plotly charts + DataFrames + Custom HTML cards)
```

---

## 5. Data Sources Used

| Source | Stage | Format | Records |
|:-------|:------|:-------|:--------|
| `reports/spark_results/query01_*` | 7 | CSV | 93 localities |
| `reports/spark_results/query02_*` | 7 | CSV | 20 cuisines |
| `reports/spark_results/query03_*` | 7 | CSV | 92 rated localities |
| `reports/spark_results/query04_*` | 7 | CSV | Top 10 restaurants |
| `reports/spark_results/query05_*` | 7 | CSV | 93 cost averages |
| `reports/spark_results/query06_*` | 7 | CSV | 93 delivery stats |
| `reports/spark_results/query07_*` | 7 | CSV | 5 price tiers |
| `reports/spark_results/query08_*` | 7 | CSV | 1 correlation row |
| `dataset/processed/clustered_restaurants/` | 8 | Parquet | 51,656 rows |
| `reports/streaming/overall_summary/` | 9 | CSV | 1 aggregate row |
| `reports/streaming/restaurant_summary/` | 9 | CSV | 10 restaurant rows |
| `reports/streaming/status_summary/` | 9 | CSV | 4 status rows |
| `reports/streaming/processed_orders/` | 9 | Parquet | 50 orders |

---

## 6. Page Descriptions

### 6.1 Home / Executive Overview (`Home.py`)

**Purpose:** Landing dashboard delivering macro-level KPIs and four key visualizations.

**KPI Cards:**
- Total Restaurants: **51,656**
- Bangalore Localities: **93**
- Weighted Average Rating: **3.70 ★**
- Weighted Average Cost for Two: **₹555**
- Online Delivery Adoption: **58.9%**
- Live Stream GMV: **₹50,282.49**

**Visualizations:**
1. Top 10 Localities by Restaurant Concentration (horizontal bar, Query 01)
2. Top 10 Dominant Cuisines (horizontal bar, Query 02)
3. Price Tier Distribution – Donut chart (Query 07)
4. Top 10 Premium Locality Costs (bar chart, Query 05)
5. Big Data Architecture 4-column summary table

---

### 6.2 Restaurant Analytics (`1_Restaurant_Analytics.py`)

**Purpose:** Provides restaurant-level performance insights.

**Sections:**
- KPI summary (peak rating 4.9, max votes 16,832, Pearson r = 0.434, 41,640 rated outlets)
- Top 10 rated restaurants (bar, Query 04)
- Locality rating extremes: top 5 vs bottom 5 (Lavelle Road 4.14 ★ vs Bommanahalli 3.19 ★)
- Correlation insight (Pearson r = +0.434, Spark Query 08)
- Interactive filterable restaurant explorer (search + locality + rating + votes sliders → up to 100 results)

---

### 6.3 Cuisine & Location Analysis (`2_Cuisine_Location_Analysis.py`)

**Purpose:** Geographic and cuisine distribution deep-dive.

**Sections:**
- KPI summary (North Indian #1 at 21,069 outlets, BTM highest density at 5,114, Lavelle Road 4.14 ★)
- Top 20 popular cuisines (full horizontal bar, Query 02)
- Top 15 restaurant concentration hubs (bar, Query 01)
- Density vs Rating Quality scatter matrix (bubble size = rated outlet count)
- Interactive Locality Deep-Dive: select 1 of 93 localities → 4 KPIs + top 10 most-voted restaurants

---

### 6.4 Pricing & Delivery Analytics (`3_Pricing_Delivery_Analytics.py`)

**Purpose:** Economic distribution and digital ordering adoption analysis.

**Sections:**
- KPI summary (33,255 budget outlets, 5,274 premium/expensive, Sankey Road ₹2,506, 30,416 delivery-enabled)
- Price tier definitions table (Budget ≤₹500 / Moderate / Expensive / Premium >₹2,000)
- Price tier donut chart (Query 07)
- Cost polarization: top 10 expensive vs top 10 affordable localities (Query 05)
- Online delivery: 10 highest-adoption vs 10 lowest-adoption localities (significant sample size ≥ 50)

---

### 6.5 Restaurant Recommendation Engine (`4_Restaurant_Recommendation.py`)

**Purpose:** Interactive ML-powered recommendation using the Stage 8 weighted formula.

**Inputs (sidebar):**
- Preferred cuisine (dropdown of 108 unique cuisine types)
- Preferred locality (dropdown of 93 Bangalore localities)
- Maximum budget for two (₹, number input)
- Minimum rating (slider 0.0–5.0)
- Delivery preference (any / yes / no)
- Number of recommendations (5–20)

**Recommendation Formula:**
$$\text{Score} = \left(0.40 \cdot S_{\text{cuisine}} + 0.25 \cdot S_{\text{rating}} + 0.15 \cdot S_{\text{budget}} + 0.10 \cdot S_{\text{delivery}} + 0.10 \cdot S_{\text{location}}\right) \times 100$$

**Outputs:**
- Result summary (total catalog, matching count, % match, recommendations shown)
- Score visualization bar chart
- Cluster segment pie chart
- Top 5 restaurant detail cards with score badge
- Full ranked results table

---

### 6.6 Live Order Monitoring (`5_Live_Order_Monitoring.py`)

**Purpose:** Visualizes the Stage 9 simulated streaming analytics snapshot.

> [!NOTE]
> This is **not** a live feed. It represents the final consolidated state of **50 synthetic orders** produced by `order_generator.py` across 10 JSON micro-batches and processed by `save_streaming_analytics.py` using Spark Structured Streaming.

**KPI Cards:**
- Total Orders: **50**
- Gross GMV: **₹50,282.49**
- Average Order Value: **₹1,005.65**
- Top Revenue Partner: **REST004 (₹10,871)**
- Delivered Orders: **17 (34%)**

**Visualizations:**
1. Orders per restaurant (horizontal bar)
2. Revenue per restaurant (horizontal bar)
3. Order status distribution (donut: 17 Delivered, 16 Out for Delivery, 9 Placed, 8 Preparing)
4. Average Order Value per restaurant (bar)
5. Order amount timeline scatter (with AOV horizontal reference line)
6. Interactive order log table with search + restaurant + status filters

---

## 7. Recommendation Engine Design

### Filter Logic (matches Stage 8 `recommend_restaurants.py`)

```python
# Cuisine: case-insensitive substring
df[cuisines.str.lower().str.contains(cuisine.lower())]

# Location: case-insensitive substring  
df[location_clean.str.lower().str.contains(location.lower())]

# Budget: hard filter
df[cost_for_two_num <= budget]

# Min Rating: hard filter
df[rating_num >= min_rating]

# Delivery: 'yes' → online_order_bool == True
#           'no'  → online_order_bool == False
#           'any' → no filter applied
```

### Score Components

| Component | Symbol | Formula | Range |
|:----------|:-------|:--------|:------|
| Cuisine Score | $S_c$ | 1.0 (passes filter) | 1.0 |
| Rating Score | $S_r$ | min(1.0, max(0.0, rating / 5.0)) | 0.0–1.0 |
| Budget Score | $S_b$ | max(0.0, 1.0 − cost / budget) | 0.0–1.0 |
| Delivery Score | $S_d$ | 1.0 (passes filter) | 1.0 |
| Location Score | $S_l$ | 1.0 (passes filter) | 1.0 |

**Score range: 65.0 – 100.0** (minimum when rating = 0.0, budget fully consumed; maximum when rating = 5.0 and cost = 0).

---

## 8. K-Means Cluster Profiles (Stage 8 Output)

| Cluster ID | Segment Name | Characteristics |
|:----------:|:-------------|:----------------|
| 0 | Budget-Friendly Delivery Gems | Low cost (≤₹400), high delivery adoption, quick-service |
| 1 | Fine Dining & Premium Lounges | Cost >₹1,500, table booking, international cuisine |
| 2 | Popular Casual & Family Dine-Out | Mid-tier (₹500–₹900), North Indian / Chinese / South Indian |
| 3 | Trending Cafes & Youth Hangouts | High-rated cafes, bakeries, contemporary social spots |
| 4 | Iconic Nightlife & Microbreweries | 5,000+ votes, 4.5+ rating, brewery/pub experience |

---

## 9. Data Loading Performance Strategy

| Data Source | Load Mechanism | Avg Load Time |
|:------------|:--------------|:--------------|
| Spark Query CSVs (Q1–Q8) | `pd.read_csv()` via glob | < 50ms per query |
| Clustered Restaurants Parquet | `pd.read_parquet(columns=[...])` | ~200ms first load |
| Streaming CSVs (summaries) | `pd.read_csv()` | < 20ms each |
| Processed Orders Parquet | `pd.read_parquet()` | < 100ms |

All loaders decorated with `@st.cache_data(ttl=3600)` — subsequent page renders serve from memory in **< 1ms**.

---

## 10. UI / UX Design Specification

### Color Palette

| Use | Color | Hex Code |
|:----|:------|:---------|
| Primary Red | Zomato Brand | `#E23744` |
| Primary Red Hover | Darker Red | `#CB202D` |
| Success Green | Positive KPIs | `#10B981` |
| Warning Amber | Moderate / Alert | `#F59E0B` |
| Info Blue | Information | `#3B82F6` |
| Accent Purple | ML/Advanced | `#8B5CF6` |
| Background | Off White | `#F9FAFB` |

### Typography
- Font: **Inter** (Google Fonts CDN) with fallback to system UI sans-serif.

### Key UI Components
- **KPI Cards**: Rounded corners (14px), red top border accent, hover lift effect.
- **Section Cards**: Soft border with subtle box shadow, standardized header.
- **Restaurant Cards**: Left red border accent, score badge (top-right), meta row.
- **Status Badges**: Color-coded pills (green: Delivered, blue: Out for Delivery, amber: Preparing, purple: Placed).
- **CTA Button**: Zomato red fill, white text, hover elevation.

---

## 11. How to Launch the Application

### Prerequisites
Ensure the virtual environment and dependencies are installed:

```bash
# WSL2 Ubuntu
source ~/venvs/foodieanalytics-spark/bin/activate

# Install if not already present
pip install streamlit plotly pandas pyarrow
```

### Launch Command

```bash
cd /mnt/c/BDA-miniproject
source ~/venvs/foodieanalytics-spark/bin/activate
streamlit run app/Home.py
```

The application will start at `http://localhost:8501`.

### Navigation

| Page | URL Path | Icon |
|:-----|:---------|:-----|
| Home / Overview | `/` | 🍔 |
| Restaurant Analytics | `/Restaurant_Analytics` | ⭐ |
| Cuisine & Location | `/Cuisine_Location_Analysis` | 🗺️ |
| Pricing & Delivery | `/Pricing_Delivery_Analytics` | 💳 |
| Recommendation Engine | `/Restaurant_Recommendation` | 🤖 |
| Live Order Monitoring | `/Live_Order_Monitoring` | ⚡ |

---

## 12. Integration Test Results

Executed via `test_stage10_integration.py`:

```
[1] Spark SQL Query Results: ALL 8 QUERIES OK (93–51,656 rows)
[2] Clustered Restaurants: (51656, 9) | 93 locations, 5 clusters
[3] Streaming Summaries: overall (1,3), restaurant (10,4), status (4,3)
[4] Processed Orders: (50, 6) | GMV: ₹50,282.49
[5] Recommendation Engine: 93 localities, 108 cuisines
    Test (Chinese/BTM/₹800/3.5+): 1,242 matches
    Top Result: Kannur Food Point - Score: 91.25

INTEGRATION TEST COMPLETED SUCCESSFULLY
```

---

## 13. Limitations and Design Decisions

| Decision | Rationale |
|:---------|:----------|
| No live Spark Session at runtime | Avoids JVM startup overhead; precomputed Parquet/CSV serve all analytics instantly |
| Pandas + PyArrow for Parquet loading | 10–50x faster than PySpark DataFrame API for single-node reads |
| `VectorUDT` columns excluded from Parquet read | Avoids complex ML feature vector deserialization; scalar columns are sufficient for the UI |
| 50 simulated orders (not continuous streaming) | Stage 9 implemented file-based batch simulation; dashboard shows the final consolidated snapshot |
| Fixed recommendation component scores | Post-filter, cuisine/location/delivery components are always 1.0; rating and budget provide continuous differentiation |
| City scope: Bangalore only | Dataset is Bangalore-specific; no multi-city support required |

---

## 14. Files Created in Stage 10

```
app/
├── Home.py
├── pages/
│   ├── 1_Restaurant_Analytics.py
│   ├── 2_Cuisine_Location_Analysis.py
│   ├── 3_Pricing_Delivery_Analytics.py
│   ├── 4_Restaurant_Recommendation.py
│   └── 5_Live_Order_Monitoring.py
├── utils/
│   ├── __init__.py
│   ├── data_loader.py
│   └── recommendation_engine.py
└── assets/
    └── styles.css

docs/stage10_dashboard.md              ← This document
requirements.txt                       ← Updated with Streamlit, Plotly, PyArrow
README.md                              ← Updated with Stage 10 completion
```

