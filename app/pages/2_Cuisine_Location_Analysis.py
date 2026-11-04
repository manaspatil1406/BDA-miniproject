"""
FoodieAnalytics - Cuisine & Location Analysis Page
Stage 10 - Interactive Dashboard and Final Application
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from utils.data_loader import (
    apply_custom_css,
    load_spark_query,
    load_clustered_restaurants
)

st.set_page_config(
    page_title="Cuisine & Location Analysis | FoodieAnalytics",
    page_icon="🗺️",
    layout="wide"
)

apply_custom_css()

# Header
st.markdown("""
    <div class="header-banner">
        <h1>🗺️ Geographic & Cuisine Distribution Analysis</h1>
        <p>
            Exploration of culinary preferences, dining clusters, and geographic concentration 
            across 93 distinct Bangalore localities aggregated from 51,656 restaurant records.
        </p>
    </div>
""", unsafe_allow_html=True)

# Load Query Data
df_loc = load_spark_query(1)       # Query 01: Restaurants by Location
df_cuisines = load_spark_query(2)  # Query 02: Popular Cuisines
df_ratings = load_spark_query(3)   # Query 03: Avg Rating by Location
df_cost = load_spark_query(5)      # Query 05: Avg Cost by Location
df_delivery = load_spark_query(6)  # Query 06: Online Delivery Analysis

# Top KPI Summary Cards
col1, col2, col3, col4 = st.columns(4)

with col1:
    top_cuisine = df_cuisines.iloc[0]["cuisine"] if not df_cuisines.empty else "North Indian"
    top_c_count = int(df_cuisines.iloc[0]["restaurant_count"]) if not df_cuisines.empty else 21069
    st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">#1 Cuisine in Bangalore</div>
            <div class="kpi-value">{top_cuisine}</div>
            <div class="kpi-subtitle">🍲 {top_c_count:,} Outlets (40.8% of market)</div>
        </div>
    """, unsafe_allow_html=True)

with col2:
    top_loc = df_loc.iloc[0]["location"] if not df_loc.empty else "BTM"
    top_loc_count = int(df_loc.iloc[0]["total_restaurants"]) if not df_loc.empty else 5114
    st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Highest Density Hub</div>
            <div class="kpi-value">{top_loc}</div>
            <div class="kpi-subtitle">🏢 {top_loc_count:,} Registered Eateries</div>
        </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
        <div class="kpi-card">
            <div class="kpi-title">Highest Rated Locality</div>
            <div class="kpi-value">Lavelle Road</div>
            <div class="kpi-subtitle">⭐ 4.14 / 5.0 (485 Outlets)</div>
        </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown("""
        <div class="kpi-card">
            <div class="kpi-title">Cuisine Diversity</div>
            <div class="kpi-value">100+</div>
            <div class="kpi-subtitle">🍜 Multi-regional Global Options</div>
        </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Section 1: Popular Cuisines (Query 02)
st.markdown("""
    <div class="section-card">
        <div class="section-title">🍲 Top 20 Most Popular Cuisines in Bangalore (Query 02)</div>
        <div class="section-subtitle">
            Tokenized and aggregated across restaurant cuisine lists using Spark SQL
        </div>
    </div>
""", unsafe_allow_html=True)

if not df_cuisines.empty:
    df_c20 = df_cuisines.head(20).copy()
    fig_cuisines_full = px.bar(
        df_c20.sort_values(by="restaurant_count", ascending=True),
        x="restaurant_count",
        y="cuisine",
        orientation="h",
        color="restaurant_count",
        color_continuous_scale="Reds",
        text="restaurant_count",
        labels={"restaurant_count": "Total Restaurants", "cuisine": "Cuisine"}
    )
    fig_cuisines_full.update_layout(
        margin=dict(l=20, r=20, t=10, b=20),
        coloraxis_showscale=False,
        height=520,
        font=dict(family="Inter, sans-serif")
    )
    fig_cuisines_full.update_traces(textposition="outside", cliponaxis=False)
    st.plotly_chart(fig_cuisines_full, use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)

# Section 2: Locality Analysis & Density vs Quality
col_loc_chart, col_matrix = st.columns([1, 1])

with col_loc_chart:
    st.markdown("""
        <div class="section-card">
            <div class="section-title">📍 Top 15 Restaurant Hubs (Query 01)</div>
            <div class="section-subtitle">Concentration of restaurants by Bangalore locality</div>
        </div>
    """, unsafe_allow_html=True)
    
    if not df_loc.empty:
        df_l15 = df_loc.head(15).copy()
        fig_l15 = px.bar(
            df_l15.sort_values(by="total_restaurants", ascending=True),
            x="total_restaurants",
            y="location",
            orientation="h",
            color="total_restaurants",
            color_continuous_scale="Purp",
            text="total_restaurants",
            labels={"total_restaurants": "Restaurants Count", "location": "Locality"}
        )
        fig_l15.update_layout(
            margin=dict(l=20, r=20, t=10, b=20),
            coloraxis_showscale=False,
            height=460,
            font=dict(family="Inter, sans-serif")
        )
        fig_l15.update_traces(textposition="outside", cliponaxis=False)
        st.plotly_chart(fig_l15, use_container_width=True)

with col_matrix:
    st.markdown("""
        <div class="section-card">
            <div class="section-title">⚖️ Density vs Rating Quality Matrix</div>
            <div class="section-subtitle">Restaurant volume vs average rating per locality</div>
        </div>
    """, unsafe_allow_html=True)
    
    # Merge Query 01 (counts) with Query 03 (ratings)
    if not df_loc.empty and not df_ratings.empty:
        merged_loc = pd.merge(df_loc, df_ratings, on="location", how="inner")
        fig_matrix = px.scatter(
            merged_loc,
            x="total_restaurants",
            y="average_rating",
            size="rated_restaurant_count",
            hover_name="location",
            color="average_rating",
            color_continuous_scale="Viridis",
            labels={
                "total_restaurants": "Total Restaurants in Locality",
                "average_rating": "Average Rating (out of 5.0)"
            }
        )
        fig_matrix.update_layout(
            margin=dict(l=20, r=20, t=10, b=20),
            height=460,
            font=dict(family="Inter, sans-serif")
        )
        st.plotly_chart(fig_matrix, use_container_width=True)

# Section 3: Interactive Locality Deep-Dive Explorer
st.markdown("<br>", unsafe_allow_html=True)
st.markdown("""
    <div class="section-card">
        <div class="section-title">🔍 Comprehensive Locality Deep-Dive Profile</div>
        <div class="section-subtitle">Select any of Bangalore's 93 localities for consolidated analytics</div>
    </div>
""", unsafe_allow_html=True)

# Merge all locality metrics: Query 1, 3, 5, 6
m1 = pd.merge(df_loc, df_ratings[["location", "average_rating"]], on="location", how="left")
m2 = pd.merge(m1, df_cost[["location", "average_cost_for_two"]], on="location", how="left")
locality_master = pd.merge(m2, df_delivery[["location", "online_order_percentage"]], on="location", how="left")

all_loc_list = sorted(locality_master["location"].unique())
chosen_loc = st.selectbox("Choose a Bangalore Locality to inspect:", all_loc_list, index=all_loc_list.index("Koramangala 5th Block") if "Koramangala 5th Block" in all_loc_list else 0)

loc_row = locality_master[locality_master["location"] == chosen_loc].iloc[0]

# Display 4 locality KPIs
lk1, lk2, lk3, lk4 = st.columns(4)
with lk1:
    st.metric("Total Restaurants", f"{int(loc_row['total_restaurants']):,}")
with lk2:
    st.metric("Average Rating", f"{loc_row['average_rating']:.2f} ★")
with lk3:
    cost_val = loc_row['average_cost_for_two']
    st.metric("Avg Cost for Two", f"₹{int(cost_val):,}" if pd.notna(cost_val) else "N/A")
with lk4:
    deliv_val = loc_row['online_order_percentage']
    st.metric("Online Delivery %", f"{deliv_val:.1f}%" if pd.notna(deliv_val) else "N/A")

# Show sample restaurants in this locality
df_clustered = load_clustered_restaurants()
loc_samples = df_clustered[df_clustered["location_clean"] == chosen_loc].sort_values(by="votes_num", ascending=False).head(10)

st.markdown(f"**Top 10 Most Voted Outlets in {chosen_loc}:**")
st.dataframe(
    loc_samples[["name", "cuisines", "rating_num", "votes_num", "cost_for_two_num", "online_order_bool"]].rename(columns={
        "name": "Restaurant Name",
        "cuisines": "Cuisines",
        "rating_num": "Rating (★)",
        "votes_num": "Votes",
        "cost_for_two_num": "Cost for Two (₹)",
        "online_order_bool": "Online Delivery"
    }),
    use_container_width=True,
    hide_index=True
)

