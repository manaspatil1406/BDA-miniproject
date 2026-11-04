"""
FoodieAnalytics - Pricing & Delivery Analytics Page
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
    page_title="Pricing & Delivery Analytics | FoodieAnalytics",
    page_icon="💳",
    layout="wide"
)

apply_custom_css()

# Header
st.markdown("""
    <div class="header-banner">
        <h1>💳 Pricing Tiers & Delivery Adoption Analytics</h1>
        <p>
            Economic distribution of dining costs, affordability segments, and 
            digital ordering adoption across Bangalore restaurants.
        </p>
    </div>
""", unsafe_allow_html=True)

# Load Spark Queries
df_cost = load_spark_query(5)      # Query 05: Average Cost by Location
df_delivery = load_spark_query(6)  # Query 06: Online Delivery Analysis
df_price = load_spark_query(7)     # Query 07: Price Range Distribution

# KPI Cards
col1, col2, col3, col4 = st.columns(4)

with col1:
    budget_count = df_price[df_price["price_category"] == "Budget"]["total_restaurants"].iloc[0] if not df_price.empty else 33255
    st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Budget Eateries (≤ ₹500)</div>
            <div class="kpi-value">{budget_count:,}</div>
            <div class="kpi-subtitle">🏷️ 64.4% of total Bangalore market</div>
        </div>
    """, unsafe_allow_html=True)

with col2:
    expensive_count = df_price[df_price["price_category"].isin(["Expensive", "Premium"])]["total_restaurants"].sum() if not df_price.empty else 5274
    st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Luxury & Premium Outlets</div>
            <div class="kpi-value">{expensive_count:,}</div>
            <div class="kpi-subtitle">🍷 Costs exceeding ₹1,000 for two</div>
        </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
        <div class="kpi-card">
            <div class="kpi-title">Priciest Bangalore Hub</div>
            <div class="kpi-value">₹2,506</div>
            <div class="kpi-subtitle">👑 Sankey Road Average Cost</div>
        </div>
    """, unsafe_allow_html=True)

with col4:
    total_online = df_delivery["online_order_restaurants"].sum() if not df_delivery.empty else 30416
    st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Online Delivery Active</div>
            <div class="kpi-value">{total_online:,}</div>
            <div class="kpi-subtitle">🛵 Enabled for digital delivery</div>
        </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Section 1: Price Category Distribution (Query 07)
col_p1, col_p2 = st.columns([1, 1])

with col_p1:
    st.markdown("""
        <div class="section-card">
            <div class="section-title">📊 Price Range Distribution (Query 07)</div>
            <div class="section-subtitle">Categorized by Spark SQL logic into 4 price tiers</div>
        </div>
    """, unsafe_allow_html=True)
    
    if not df_price.empty:
        tier_colors = {
            "Budget": "#10B981",
            "Moderate": "#3B82F6",
            "Expensive": "#F59E0B",
            "Premium": "#E23744",
            "Unknown": "#9CA3AF"
        }
        fig_donut = px.pie(
            df_price,
            names="price_category",
            values="total_restaurants",
            color="price_category",
            color_discrete_map=tier_colors,
            hole=0.45
        )
        fig_donut.update_layout(
            margin=dict(l=20, r=20, t=10, b=20),
            height=360,
            font=dict(family="Inter, sans-serif")
        )
        st.plotly_chart(fig_donut, use_container_width=True)

with col_p2:
    st.markdown("""
        <div class="section-card">
            <div class="section-title">💡 Price Tier Thresholds & Counts</div>
            <div class="section-subtitle">Definitions based on cost for two persons in INR</div>
        </div>
    """, unsafe_allow_html=True)
    
    tier_info = pd.DataFrame([
        {"Category": "Budget", "Cost Range": "≤ ₹500", "Count": "33,255", "Share": "64.38%"},
        {"Category": "Moderate", "Cost Range": "₹501 - ₹1,000", "Count": "12,803", "Share": "24.79%"},
        {"Category": "Expensive", "Cost Range": "₹1,001 - ₹2,000", "Count": "4,650", "Share": "9.00%"},
        {"Category": "Premium", "Cost Range": "> ₹2,000", "Count": "624", "Share": "1.21%"},
        {"Category": "Unknown", "Cost Range": "Unspecified", "Count": "324", "Share": "0.62%"}
    ])
    st.table(tier_info)

# Section 2: Locality Cost Extremes (Query 05)
st.markdown("<br>", unsafe_allow_html=True)
st.markdown("""
    <div class="section-card">
        <div class="section-title">💰 Locality Cost Polarization (Query 05)</div>
        <div class="section-subtitle">Comparison between the 10 most expensive and 10 most affordable localities</div>
    </div>
""", unsafe_allow_html=True)

if not df_cost.empty:
    top_cost = df_cost.sort_values(by="average_cost_for_two", ascending=False).head(10).copy()
    top_cost["Tier"] = "Most Expensive"
    bottom_cost = df_cost.sort_values(by="average_cost_for_two", ascending=True).head(10).copy()
    bottom_cost["Tier"] = "Most Affordable"
    cost_extremes = pd.concat([top_cost, bottom_cost]).sort_values(by="average_cost_for_two", ascending=True)
    
    fig_cost_extremes = px.bar(
        cost_extremes,
        x="average_cost_for_two",
        y="location",
        orientation="h",
        color="Tier",
        color_discrete_map={"Most Expensive": "#E23744", "Most Affordable": "#10B981"},
        text="average_cost_for_two",
        labels={"average_cost_for_two": "Average Cost for Two (₹)", "location": "Locality"}
    )
    fig_cost_extremes.update_layout(
        margin=dict(l=20, r=20, t=10, b=20),
        height=480,
        font=dict(family="Inter, sans-serif")
    )
    fig_cost_extremes.update_traces(texttemplate='₹%{x:.0f}', textposition="outside", cliponaxis=False)
    st.plotly_chart(fig_cost_extremes, use_container_width=True)

# Section 3: Online Delivery Adoption by Locality (Query 06)
st.markdown("<br>", unsafe_allow_html=True)
col_del_high, col_del_low = st.columns([1, 1])

with col_del_high:
    st.markdown("""
        <div class="section-card">
            <div class="section-title">🚀 Highest Online Delivery Adoption (Query 06)</div>
            <div class="section-subtitle">Localities where digital ordering adoption exceeds 75%</div>
        </div>
    """, unsafe_allow_html=True)
    
    if not df_delivery.empty:
        # Filter for localities with meaningful sample size (e.g. >= 50 restaurants)
        sig_delivery = df_delivery[df_delivery["total_restaurants"] >= 50].sort_values(by="online_order_percentage", ascending=False).head(10)
        fig_del_high = px.bar(
            sig_delivery.sort_values(by="online_order_percentage", ascending=True),
            x="online_order_percentage",
            y="location",
            orientation="h",
            color="online_order_percentage",
            color_continuous_scale="Teal",
            text="online_order_percentage",
            labels={"online_order_percentage": "Delivery Adoption %", "location": "Locality"}
        )
        fig_del_high.update_layout(
            margin=dict(l=20, r=20, t=10, b=20),
            coloraxis_showscale=False,
            height=380,
            font=dict(family="Inter, sans-serif")
        )
        fig_del_high.update_traces(texttemplate='%{x:.1f}%', textposition="outside", cliponaxis=False)
        st.plotly_chart(fig_del_high, use_container_width=True)

with col_del_low:
    st.markdown("""
        <div class="section-card">
            <div class="section-title">🏬 Lowest Online Delivery Adoption (Query 06)</div>
            <div class="section-subtitle">Localities dominated by traditional dine-in or retail</div>
        </div>
    """, unsafe_allow_html=True)
    
    if not df_delivery.empty:
        low_delivery = df_delivery[df_delivery["total_restaurants"] >= 50].sort_values(by="online_order_percentage", ascending=True).head(10)
        fig_del_low = px.bar(
            low_delivery.sort_values(by="online_order_percentage", ascending=True),
            x="online_order_percentage",
            y="location",
            orientation="h",
            color="online_order_percentage",
            color_continuous_scale="Reds",
            text="online_order_percentage",
            labels={"online_order_percentage": "Delivery Adoption %", "location": "Locality"}
        )
        fig_del_low.update_layout(
            margin=dict(l=20, r=20, t=10, b=20),
            coloraxis_showscale=False,
            height=380,
            font=dict(family="Inter, sans-serif")
        )
        fig_del_low.update_traces(texttemplate='%{x:.1f}%', textposition="outside", cliponaxis=False)
        st.plotly_chart(fig_del_low, use_container_width=True)

