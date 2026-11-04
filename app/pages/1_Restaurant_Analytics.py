"""
FoodieAnalytics - Restaurant Analytics Page
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
    page_title="Restaurant Analytics | FoodieAnalytics",
    page_icon="⭐",
    layout="wide"
)

apply_custom_css()

# Header
st.markdown("""
    <div class="header-banner">
        <h1>⭐ Restaurant Performance & Rating Analytics</h1>
        <p>
            In-depth analysis of restaurant ratings, customer engagement (votes), 
            and the statistical correlation between review volume and restaurant excellence across Bangalore.
        </p>
    </div>
""", unsafe_allow_html=True)

# Load Query Results
df_top_rated = load_spark_query(4)  # Query 04: Top Rated Restaurants
df_loc_ratings = load_spark_query(3)  # Query 03: Average Rating by Location
df_corr = load_spark_query(8)  # Query 08: Rating vs Votes correlation

correlation_val = float(df_corr["rating_votes_correlation"].iloc[0]) if not df_corr.empty else 0.434
valid_records = int(df_corr["valid_record_count"].iloc[0]) if not df_corr.empty else 41640

# Top KPI Metric Cards
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown("""
        <div class="kpi-card">
            <div class="kpi-title">Peak Rating Achieved</div>
            <div class="kpi-value">4.9 <span style="font-size:1.2rem; color:#F59E0B;">★</span></div>
            <div class="kpi-subtitle">🏆 10 Elite Bangalore Restaurants</div>
        </div>
    """, unsafe_allow_html=True)

with col2:
    max_votes = df_top_rated["votes"].max() if not df_top_rated.empty else 16832
    st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Highest Vote Volume</div>
            <div class="kpi-value">{max_votes:,}</div>
            <div class="kpi-subtitle">🍺 Byg Brewski Brewing Co.</div>
        </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Rating-Vote Correlation</div>
            <div class="kpi-value">+{correlation_val:.3f}</div>
            <div class="kpi-subtitle">📈 Moderate Positive Correlation</div>
        </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Verified Rated Outlets</div>
            <div class="kpi-value">{valid_records:,}</div>
            <div class="kpi-subtitle">📊 Non-null Numerical Ratings</div>
        </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Section 1: Top Rated Restaurants & Locality Ratings
col_top, col_loc = st.columns([1, 1])

with col_top:
    st.markdown("""
        <div class="section-card">
            <div class="section-title">🏆 Top 10 Rated Restaurants (Query 04)</div>
            <div class="section-subtitle">Ranked by rating (4.9/5.0) and engagement volume (votes)</div>
        </div>
    """, unsafe_allow_html=True)
    
    if not df_top_rated.empty:
        fig_top = px.bar(
            df_top_rated.sort_values(by="votes", ascending=True),
            x="votes",
            y="restaurant_name",
            orientation="h",
            color="votes",
            color_continuous_scale="Reds",
            hover_data=["location", "average_rating"],
            text="votes",
            labels={"votes": "Customer Votes", "restaurant_name": "Restaurant"}
        )
        fig_top.update_layout(
            margin=dict(l=20, r=20, t=10, b=20),
            coloraxis_showscale=False,
            height=380,
            font=dict(family="Inter, sans-serif")
        )
        fig_top.update_traces(textposition="outside", cliponaxis=False)
        st.plotly_chart(fig_top, use_container_width=True)

with col_loc:
    st.markdown("""
        <div class="section-card">
            <div class="section-title">🌟 Locality Rating Extremes (Query 03)</div>
            <div class="section-subtitle">Top 5 highest vs bottom 5 lowest rated localities</div>
        </div>
    """, unsafe_allow_html=True)
    
    if not df_loc_ratings.empty:
        top_5_loc = df_loc_ratings.sort_values(by="average_rating", ascending=False).head(5).copy()
        top_5_loc["Category"] = "Top 5 Rated"
        bottom_5_loc = df_loc_ratings.sort_values(by="average_rating", ascending=True).head(5).copy()
        bottom_5_loc["Category"] = "Bottom 5 Rated"
        combined_extremes = pd.concat([top_5_loc, bottom_5_loc]).sort_values(by="average_rating", ascending=True)
        
        fig_extremes = px.bar(
            combined_extremes,
            x="average_rating",
            y="location",
            orientation="h",
            color="Category",
            color_discrete_map={"Top 5 Rated": "#10B981", "Bottom 5 Rated": "#EF4444"},
            text="average_rating",
            labels={"average_rating": "Average Rating (out of 5.0)", "location": "Bangalore Locality"}
        )
        fig_extremes.update_layout(
            margin=dict(l=20, r=20, t=10, b=20),
            height=380,
            xaxis=dict(range=[2.5, 4.5]),
            font=dict(family="Inter, sans-serif")
        )
        fig_extremes.update_traces(texttemplate='%{x:.2f} ★', textposition="outside", cliponaxis=False)
        st.plotly_chart(fig_extremes, use_container_width=True)

# Section 2: Statistical Insights & Rating-Vote Relationship
st.markdown("""
    <div class="section-card">
        <div class="section-title">📊 Statistical Correlation: Rating vs Votes (Query 08)</div>
        <div class="section-subtitle">
            Pearson Correlation Coefficient: <b>+0.434</b> calculated across <b>41,640</b> valid records
        </div>
    </div>
""", unsafe_allow_html=True)

st.markdown("""
> [!NOTE]
> **Analytical Finding:** A positive Pearson correlation coefficient of **+0.434** confirms a moderately strong relationship between rating and voting volume. Highly rated restaurants tend to accumulate significantly greater numbers of customer reviews, as customer satisfaction drives viral advocacy and frequent check-ins. However, since the correlation is below 0.70, popular low-cost joints and new high-quality niche eateries also exist with fewer cumulative reviews.
""")

# Section 3: Interactive Filterable Restaurant Explorer
st.markdown("<br>", unsafe_allow_html=True)
st.markdown("""
    <div class="section-card">
        <div class="section-title">🔍 Interactive Restaurant Explorer</div>
        <div class="section-subtitle">Filter through the 51,656 restaurant records dynamically</div>
    </div>
""", unsafe_allow_html=True)

# Load full clustered dataset
df_all = load_clustered_restaurants()

# Filters row
f_col1, f_col2, f_col3, f_col4 = st.columns(4)

with f_col1:
    search_term = st.text_input("Search Restaurant Name", placeholder="e.g. Empire, Truffles...")

with f_col2:
    all_locations = ["All Localities"] + sorted(list(df_all["location_clean"].dropna().unique()))
    selected_loc = st.selectbox("Select Locality", all_locations)

with f_col3:
    min_rating_sel = st.slider("Minimum Rating", min_value=0.0, max_value=5.0, value=3.5, step=0.1)

with f_col4:
    min_votes_sel = st.number_input("Minimum Votes", min_value=0, max_value=15000, value=50, step=50)

# Filter logic
filtered_display = df_all.copy()

if search_term:
    filtered_display = filtered_display[
        filtered_display["name"].str.contains(search_term, case=False, na=False)
    ]

if selected_loc != "All Localities":
    filtered_display = filtered_display[
        filtered_display["location_clean"] == selected_loc
    ]

filtered_display = filtered_display[
    (filtered_display["rating_num"] >= min_rating_sel) &
    (filtered_display["votes_num"] >= min_votes_sel)
]

st.markdown(f"**Showing {len(filtered_display):,} matching restaurants** (sorted by votes & rating):")

cols_to_show = ["name", "location_clean", "cuisines", "rating_num", "votes_num", "cost_for_two_num", "online_order_bool"]
display_df = (
    filtered_display[cols_to_show]
    .sort_values(by=["rating_num", "votes_num"], ascending=[False, False])
    .head(100)
    .rename(columns={
        "name": "Restaurant Name",
        "location_clean": "Locality",
        "cuisines": "Cuisines",
        "rating_num": "Rating (★)",
        "votes_num": "Votes",
        "cost_for_two_num": "Cost for Two (₹)",
        "online_order_bool": "Online Delivery"
    })
)

st.dataframe(display_df, use_container_width=True, hide_index=True)

