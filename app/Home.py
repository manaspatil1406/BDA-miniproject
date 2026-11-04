"""
FoodieAnalytics - Home / Executive Overview Dashboard
Stage 10 - Interactive Dashboard and Final Application
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from utils.data_loader import (
    apply_custom_css,
    load_all_spark_queries,
    get_dataset_kpis,
    load_streaming_summaries
)

# Page configuration
st.set_page_config(
    page_title="FoodieAnalytics | Big Data Dashboard",
    page_icon="🍔",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply Zomato-inspired CSS styles
apply_custom_css()

# Sidebar Information
with st.sidebar:
    st.markdown("""
        <div class="sidebar-brand">
            <span>🍔 FoodieAnalytics</span>
        </div>
        <p class="sidebar-caption">
            Big Data Food Delivery Analytics & Recommendation Platform
        </p>
    """, unsafe_allow_html=True)
    
    st.info("""
    **Project Scope:**
    - City: Bangalore, India
    - Total Restaurants: 51,656
    - Total Localities: 93
    - Analytical Engines: Hadoop, Hive, Spark SQL, Spark MLlib, Structured Streaming
    """)
    
    st.markdown("### 🛠️ Technology Stack")
    st.markdown("""
    - **Apache Spark 4.0.1** (Core & SQL)
    - **Apache Hadoop 3.4.3** (HDFS & MapReduce)
    - **Apache Hive 4.0.1** (Data Warehousing)
    - **Spark MLlib** (K-Means Clustering)
    - **Spark Structured Streaming** (Live Micro-Batches)
    - **Streamlit & Plotly** (Interactive UI)
    - **Ubuntu 24.04 LTS** (WSL2 Architecture)
    """)
    
    st.markdown("---")
    st.caption("Big Data Analytics Mini Project © 2026")

# Load precomputed data
queries = load_all_spark_queries()
kpis = get_dataset_kpis()
df_overall_stream, df_rest_stream, df_status_stream = load_streaming_summaries()

# Top Hero Banner
st.markdown("""
    <div class="header-banner">
        <h1>FoodieAnalytics: Big Data Delivery & Recommendation Platform</h1>
        <p>
            An end-to-end Big Data Analytics architecture processing 51,656 Bangalore restaurants 
            across distributed storage, data warehousing, machine learning clustering, 
            and real-time micro-batch stream processing.
        </p>
        <div class="tech-badges">
            <span class="tech-badge">⚡ Apache Spark 4.0.1</span>
            <span class="tech-badge">🐘 Apache Hadoop 3.4.3</span>
            <span class="tech-badge">🐝 Apache Hive 4.0.1</span>
            <span class="tech-badge">🤖 Spark MLlib</span>
            <span class="tech-badge">🌊 Structured Streaming</span>
            <span class="tech-badge">📊 Streamlit & Plotly</span>
        </div>
    </div>
""", unsafe_allow_html=True)

# Executive KPI Metric Cards
st.markdown("### 📈 Bangalore Food Ecosystem At A Glance")

col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Total Restaurants</div>
            <div class="kpi-value">{kpis['total_restaurants']:,}</div>
            <div class="kpi-subtitle">📍 100% Validated Records</div>
        </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Bangalore Localities</div>
            <div class="kpi-value">{kpis['total_localities']}</div>
            <div class="kpi-subtitle">🏙️ Urban Neighborhoods</div>
        </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Average Rating</div>
            <div class="kpi-value">{kpis['average_rating']} <span style="font-size:1.2rem; color:#F59E0B;">★</span></div>
            <div class="kpi-subtitle">⭐ 41,665 Rated Outlets</div>
        </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Avg Cost for Two</div>
            <div class="kpi-value">₹{int(kpis['average_cost_for_two'])}</div>
            <div class="kpi-subtitle">💳 Across Valid Menus</div>
        </div>
    """, unsafe_allow_html=True)

with col5:
    st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Online Delivery</div>
            <div class="kpi-value">{kpis['online_delivery_pct']}%</div>
            <div class="kpi-subtitle">🚀 30,416 Outlets Active</div>
        </div>
    """, unsafe_allow_html=True)

with col6:
    total_rev = df_overall_stream['total_revenue'].iloc[0] if not df_overall_stream.empty else 50282.49
    st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Live Stream GMV</div>
            <div class="kpi-value">₹{int(total_rev):,}</div>
            <div class="kpi-subtitle">⚡ 50 Simulated Orders</div>
        </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Row 1 Visualizations: Locality Concentration & Popular Cuisines
col_left, col_right = st.columns(2)

with col_left:
    st.markdown("""
        <div class="section-card">
            <div class="section-title">📍 Top 10 Localities by Restaurant Concentration</div>
            <div class="section-subtitle">Aggregated via Spark SQL from 51,656 catalog entries</div>
        </div>
    """, unsafe_allow_html=True)
    
    df_loc = queries.get(1, pd.DataFrame()).head(10).copy()
    if not df_loc.empty:
        fig_loc = px.bar(
            df_loc,
            x="total_restaurants",
            y="location",
            orientation="h",
            color="total_restaurants",
            color_continuous_scale="Reds",
            text="total_restaurants",
            labels={"total_restaurants": "Restaurant Count", "location": "Bangalore Locality"}
        )
        fig_loc.update_layout(
            yaxis=dict(autorange="reversed"),
            margin=dict(l=20, r=20, t=10, b=20),
            coloraxis_showscale=False,
            height=360,
            font=dict(family="Inter, sans-serif")
        )
        fig_loc.update_traces(textposition="outside", cliponaxis=False)
        st.plotly_chart(fig_loc, use_container_width=True)

with col_right:
    st.markdown("""
        <div class="section-card">
            <div class="section-title">🍲 Top 10 Dominant Cuisines in Bangalore</div>
            <div class="section-subtitle">Extracted via Spark SQL array tokenization & counting</div>
        </div>
    """, unsafe_allow_html=True)
    
    df_cuisines = queries.get(2, pd.DataFrame()).head(10).copy()
    if not df_cuisines.empty:
        fig_cuisines = px.bar(
            df_cuisines,
            x="restaurant_count",
            y="cuisine",
            orientation="h",
            color="restaurant_count",
            color_continuous_scale="Burg",
            text="restaurant_count",
            labels={"restaurant_count": "Restaurants Offering Cuisine", "cuisine": "Cuisine Category"}
        )
        fig_cuisines.update_layout(
            yaxis=dict(autorange="reversed"),
            margin=dict(l=20, r=20, t=10, b=20),
            coloraxis_showscale=False,
            height=360,
            font=dict(family="Inter, sans-serif")
        )
        fig_cuisines.update_traces(textposition="outside", cliponaxis=False)
        st.plotly_chart(fig_cuisines, use_container_width=True)

# Row 2 Visualizations: Price Tiers & Cost by Locality
col_p1, col_p2 = st.columns([1, 1])

with col_p1:
    st.markdown("""
        <div class="section-card">
            <div class="section-title">💰 Price Tier Distribution (Budget vs Luxury)</div>
            <div class="section-subtitle">Classified via Spark SQL CASE statements</div>
        </div>
    """, unsafe_allow_html=True)
    
    df_price = queries.get(7, pd.DataFrame()).copy()
    if not df_price.empty:
        # Custom palette for price tiers
        tier_colors = {
            "Budget": "#10B981",
            "Moderate": "#3B82F6",
            "Expensive": "#F59E0B",
            "Premium": "#E23744",
            "Unknown": "#9CA3AF"
        }
        fig_price = px.pie(
            df_price,
            names="price_category",
            values="total_restaurants",
            color="price_category",
            color_discrete_map=tier_colors,
            hole=0.45
        )
        fig_price.update_layout(
            margin=dict(l=20, r=20, t=10, b=20),
            height=350,
            font=dict(family="Inter, sans-serif")
        )
        st.plotly_chart(fig_price, use_container_width=True)

with col_p2:
    st.markdown("""
        <div class="section-card">
            <div class="section-title">💎 Top 10 Most Premium Localities by Cost for Two</div>
            <div class="section-subtitle">Average meal cost for two persons in INR (Spark Query 5)</div>
        </div>
    """, unsafe_allow_html=True)
    
    df_cost = queries.get(5, pd.DataFrame()).head(10).copy()
    if not df_cost.empty:
        fig_cost = px.bar(
            df_cost,
            x="location",
            y="average_cost_for_two",
            color="average_cost_for_two",
            color_continuous_scale="OrRd",
            text="average_cost_for_two",
            labels={"average_cost_for_two": "Avg Cost for Two (₹)", "location": "Locality"}
        )
        fig_cost.update_layout(
            margin=dict(l=20, r=20, t=10, b=20),
            coloraxis_showscale=False,
            height=350,
            font=dict(family="Inter, sans-serif")
        )
        fig_cost.update_traces(texttemplate='₹%{y:.0f}', textposition="outside", cliponaxis=False)
        st.plotly_chart(fig_cost, use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)

# Big Data Architecture Flow Card
st.markdown("""
    <div class="section-card">
        <div class="section-title">🏗️ End-to-End Big Data Analytics Architecture (Stages 1–10)</div>
        <div class="section-subtitle">Comprehensive lifecycle from raw ingest to interactive intelligence</div>
    </div>
""", unsafe_allow_html=True)

arch_col1, arch_col2, arch_col3, arch_col4 = st.columns(4)

with arch_col1:
    st.markdown("""
        **1. Ingestion & Storage**
        - Raw dataset: 51,717 rows
        - Cleaned dataset: 51,656 rows (17 cols)
        - Hadoop HDFS: `/foodieanalytics/`
        - Fault-tolerant distributed replication
    """)
    st.caption("Stages 1, 2, 3")

with arch_col2:
    st.markdown("""
        **2. Batch Processing**
        - Hadoop Streaming MapReduce (4 jobs)
        - Apache Hive DW (External ORC tables)
        - Partitioning by locality & price
        - 8 Core Analytical Hive queries
    """)
    st.caption("Stages 4, 5")

with arch_col3:
    st.markdown("""
        **3. Spark Core & MLlib**
        - Apache Spark 4.0.1 DataFrames
        - Catalyst SQL queries (Queries 1–8)
        - K-Means Clustering ($k=5$)
        - Multi-criteria recommendation engine
    """)
    st.caption("Stages 6, 7, 8")

with arch_col4:
    st.markdown("""
        **4. Streaming & Visual UI**
        - Spark Structured Streaming
        - 10 File-based JSON micro-batches
        - Parquet persistence & KPI reporting
        - Streamlit + Plotly interactive UI
    """)
    st.caption("Stages 9, 10")

st.markdown("---")
st.markdown("""
    <div style="text-align: center; color: #6B7280; font-size: 0.9rem; padding: 1rem 0;">
        💡 Use the sidebar navigation to explore <b>Restaurant Analytics</b>, <b>Cuisine & Location Patterns</b>, 
        <b>Pricing & Delivery Adoption</b>, the <b>ML Recommendation Engine</b>, and <b>Live Order Monitoring</b>.
    </div>
""", unsafe_allow_html=True)

