"""
FoodieAnalytics - Restaurant Recommendation Engine Page
Stage 10 - Interactive Dashboard and Final Application

Implements multi-criteria, weighted recommendation scoring that replicates
the Stage 8 Spark MLlib pipeline entirely using Pandas + PyArrow.

Scoring Formula (from Stage 8 recommend_restaurants.py):
  Score = (0.40 * Cuisine + 0.25 * Rating + 0.15 * Budget + 0.10 * Delivery + 0.10 * Location) * 100
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from utils.data_loader import apply_custom_css, load_clustered_restaurants
from utils.recommendation_engine import (
    recommend_restaurants,
    get_filter_options,
    get_cluster_profiles
)

st.set_page_config(
    page_title="Restaurant Recommendation | FoodieAnalytics",
    page_icon="🤖",
    layout="wide"
)

apply_custom_css()

# Header
st.markdown("""
    <div class="header-banner">
        <h1>🤖 ML-Powered Restaurant Recommendation Engine</h1>
        <p>
            Multi-criteria weighted recommendation scoring derived from the K-Means clustering model (Stage 8).
            Filters 51,656 Bangalore restaurants by cuisine, locality, budget, rating, and delivery preference.
        </p>
        <div class="tech-badges">
            <span class="tech-badge">🤖 K-Means Clustering (k=5)</span>
            <span class="tech-badge">📐 Weighted Scoring Formula</span>
            <span class="tech-badge">🗄️ 51,656 Restaurant Catalog</span>
            <span class="tech-badge">🏙️ 93 Bangalore Localities</span>
        </div>
    </div>
""", unsafe_allow_html=True)

# Load data
df_catalog = load_clustered_restaurants()
locations, cuisines = get_filter_options(df_catalog)

# --- Sidebar: Preference Input Panel ---
st.sidebar.markdown("## 🎯 Your Preferences")
st.sidebar.markdown("Fill in what you're looking for to get personalized recommendations.")

cuisine_choices = ["All Cuisines"] + cuisines
loc_choices = ["All Bangalore"] + locations

sel_cuisine = st.sidebar.selectbox("🍲 Preferred Cuisine", cuisine_choices, index=0)
sel_location = st.sidebar.selectbox("📍 Preferred Locality", loc_choices, index=0)
sel_budget = st.sidebar.number_input(
    "💰 Max Budget for Two (₹)",
    min_value=0,
    max_value=10000,
    value=1000,
    step=100,
    help="Set 0 to disable budget filtering"
)
sel_rating = st.sidebar.slider(
    "⭐ Minimum Rating",
    min_value=0.0,
    max_value=5.0,
    value=3.5,
    step=0.1
)
sel_delivery = st.sidebar.radio(
    "🛵 Online Delivery Preference",
    options=["any", "yes", "no"],
    index=0,
    horizontal=True
)
sel_top_n = st.sidebar.select_slider(
    "🏆 Number of Recommendations",
    options=[5, 10, 15, 20],
    value=10
)

find_btn = st.sidebar.button("🔍 Find Recommended Restaurants", type="primary", use_container_width=True)

# --- Scoring Methodology Explainer ---
with st.expander("📐 How the Recommendation Scoring Works", expanded=False):
    col_w1, col_w2 = st.columns([1, 1])
    with col_w1:
        st.markdown("""
        **Weighted Scoring Formula (Stage 8 Specification):**
        
        $$\\text{Score} = \\left(0.40 \\cdot S_{\\text{cuisine}} + 0.25 \\cdot S_{\\text{rating}} + 0.15 \\cdot S_{\\text{budget}} + 0.10 \\cdot S_{\\text{delivery}} + 0.10 \\cdot S_{\\text{location}}\\right) \\times 100$$
        
        **Score Component Calculations:**
        
        | Component | Weight | Calculation |
        |:----------|:------:|:-----------|
        | Cuisine Match | 40% | 1.0 (restaurant passes substring filter) |
        | Rating Score | 25% | rating / 5.0 (normalized, clamped 0–1) |
        | Budget Score | 15% | max(0, 1 − cost / budget) |
        | Delivery Score | 10% | 1.0 (passes delivery filter) |
        | Location Score | 10% | 1.0 (passes locality filter) |
        """)
    with col_w2:
        st.markdown("""
        **How Filtering Works:**
        - **Cuisine** – Case-insensitive substring match against restaurant cuisines list
        - **Location** – Case-insensitive substring match against locality name
        - **Budget** – Hard filter: cost_for_two ≤ max budget (skipped if budget = 0)
        - **Min Rating** – Hard filter: rating ≥ threshold (skipped if 0.0)
        - **Delivery** – 'yes' = online only, 'no' = dine-out only, 'any' = both included
        
        **Tie-Breaking Order:**
        1. `recommendation_score` descending
        2. `rating_num` descending  
        3. `votes_num` descending
        
        > ℹ️ All restaurants passing the filters receive fixed component scores
        > (cuisine = 1.0, delivery = 1.0, location = 1.0). The differentiating
        > factor is the **rating score** (dynamic) and **budget compatibility** (dynamic).
        """)

st.markdown("<br>", unsafe_allow_html=True)

# --- Recommendations Display ---
if find_btn or "recommendations_result" in st.session_state:
    if find_btn:
        # Run recommendation logic
        budget_param = sel_budget if sel_budget > 0 else None
        rating_param = sel_rating if sel_rating > 0.0 else None
        cuisine_param = None if sel_cuisine == "All Cuisines" else sel_cuisine
        location_param = None if sel_location == "All Bangalore" else sel_location

        with st.spinner("🔍 Scoring and ranking restaurants..."):
            results, metadata = recommend_restaurants(
                df=df_catalog,
                cuisine=cuisine_param,
                location=location_param,
                budget=budget_param,
                min_rating=rating_param,
                delivery=sel_delivery,
                top_n=sel_top_n
            )

        st.session_state["recommendations_result"] = results
        st.session_state["recommendations_meta"] = metadata
        st.session_state["rec_params"] = {
            "cuisine": sel_cuisine,
            "location": sel_location,
            "budget": sel_budget,
            "rating": sel_rating,
            "delivery": sel_delivery
        }

    results = st.session_state.get("recommendations_result", pd.DataFrame())
    metadata = st.session_state.get("recommendations_meta", {})
    params = st.session_state.get("rec_params", {})

    # Result Summary Banner
    matching_count = metadata.get("matching_count", 0)
    total_count = metadata.get("total_catalog", 51656)

    col_meta1, col_meta2, col_meta3, col_meta4 = st.columns(4)
    with col_meta1:
        st.metric("Total Catalog", f"{total_count:,}")
    with col_meta2:
        st.metric("Matching Restaurants", f"{matching_count:,}")
    with col_meta3:
        pct = (matching_count / total_count * 100) if total_count else 0
        st.metric("Catalog Match %", f"{pct:.1f}%")
    with col_meta4:
        showing = min(len(results), sel_top_n) if not results.empty else 0
        st.metric("Recommendations Shown", f"{showing}")

    if results.empty:
        st.error("""
        ❌ **No restaurants matched all your preferences.**
        
        **Tips to get results:**
        - Increase the budget (try ₹2,000+)
        - Lower the minimum rating threshold
        - Choose "All Cuisines" or "All Bangalore"
        - Select "any" for delivery preference
        """)
    else:
        # Sort for display
        display_results = results.copy()

        # Score distribution bar chart
        col_chart1, col_chart2 = st.columns([2, 1])
        with col_chart1:
            st.markdown(f"""
                <div class="section-card">
                    <div class="section-title">🏆 Top {len(display_results)} Recommended Restaurants</div>
                    <div class="section-subtitle">Ranked by weighted recommendation score</div>
                </div>
            """, unsafe_allow_html=True)

            fig_rec = px.bar(
                display_results.sort_values("recommendation_score", ascending=True),
                x="recommendation_score",
                y="name",
                orientation="h",
                color="recommendation_score",
                color_continuous_scale="Reds",
                text="recommendation_score",
                hover_data=["location_clean", "cuisines", "rating_num", "cost_for_two_num"],
                labels={
                    "recommendation_score": "Recommendation Score (/100)",
                    "name": "Restaurant"
                }
            )
            fig_rec.update_layout(
                margin=dict(l=20, r=20, t=10, b=20),
                coloraxis_showscale=False,
                height=400,
                font=dict(family="Inter, sans-serif")
            )
            fig_rec.update_traces(texttemplate='%{x:.1f}', textposition="outside", cliponaxis=False)
            st.plotly_chart(fig_rec, use_container_width=True)

        with col_chart2:
            st.markdown("""
                <div class="section-card">
                    <div class="section-title">📊 Cluster Profile Breakdown</div>
                    <div class="section-subtitle">K-Means segments of recommendations</div>
                </div>
            """, unsafe_allow_html=True)

            cluster_counts = display_results["cluster_name"].value_counts().reset_index()
            cluster_counts.columns = ["Cluster", "Count"]
            fig_cl = px.pie(
                cluster_counts,
                names="Cluster",
                values="Count",
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set2
            )
            fig_cl.update_layout(
                margin=dict(l=5, r=5, t=5, b=5),
                height=350,
                font=dict(family="Inter, sans-serif")
            )
            st.plotly_chart(fig_cl, use_container_width=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Individual Restaurant Cards (top 5) + Full Table
        st.markdown("### 🃏 Top 5 Recommendation Details")
        for idx, row in display_results.head(5).iterrows():
            score_color = "#E23744" if row["recommendation_score"] >= 90 else "#F59E0B" if row["recommendation_score"] >= 75 else "#6B7280"
            delivery_icon = "✅" if row.get("online_order_bool") == True else "❌" if row.get("online_order_bool") == False else "—"
            cost_str = f"₹{int(row['cost_for_two_num']):,}" if pd.notna(row.get('cost_for_two_num')) else "N/A"
            rating_str = f"{row['rating_num']:.1f} ★" if pd.notna(row.get('rating_num')) else "Unrated"
            votes_str = f"{int(row['votes_num']):,}" if pd.notna(row.get('votes_num')) else "0"

            st.markdown(f"""
            <div class="rec-card">
                <div style="display:flex; justify-content:space-between; align-items:flex-start;">
                    <div>
                        <div class="rec-title">{row['name']}</div>
                        <div class="rec-meta">📍 {row['location_clean']}  &nbsp;|&nbsp; 🍲 {row['cuisines'][:60]}{'...' if len(str(row['cuisines'])) > 60 else ''}</div>
                        <div style="display:flex; gap:1rem; font-size:0.9rem; color:#374151;">
                            <span>⭐ {rating_str}</span>
                            <span>👥 {votes_str} votes</span>
                            <span>💰 {cost_str}</span>
                            <span>🛵 Delivery: {delivery_icon}</span>
                            <span>🤖 {row.get('cluster_name', 'Standard')}</span>
                        </div>
                    </div>
                    <div class="rec-score-pill" style="background:#FEE2E2; color:#991B1B; font-size:1.1rem; padding:0.4rem 1rem;">
                        {row['recommendation_score']:.1f} / 100
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<br>**📋 Full Recommendations Table:**")
        full_table = display_results[[
            "name", "location_clean", "cuisines", "rating_num", "votes_num",
            "cost_for_two_num", "online_order_bool", "cluster_name", "recommendation_score"
        ]].rename(columns={
            "name": "Restaurant Name",
            "location_clean": "Locality",
            "cuisines": "Cuisines",
            "rating_num": "Rating (★)",
            "votes_num": "Votes",
            "cost_for_two_num": "Cost for Two (₹)",
            "online_order_bool": "Online Delivery",
            "cluster_name": "Cluster",
            "recommendation_score": "Score (/100)"
        })
        st.dataframe(full_table, use_container_width=True, hide_index=True)

else:
    # Idle State - Cluster Profiles
    st.markdown("""
        <div class="section-card">
            <div class="section-title">🤖 K-Means Clustering: 5 Restaurant Segments (Stage 8)</div>
            <div class="section-subtitle">The recommendation engine uses clusters from Spark MLlib K-Means (k=5) trained on 51,656 restaurants</div>
        </div>
    """, unsafe_allow_html=True)

    cluster_profiles = get_cluster_profiles()
    for cid, profile in cluster_profiles.items():
        st.markdown(f"""
        <div class="rec-card">
            <div class="rec-title" style="color:{profile['color']};">Cluster {cid}: {profile['name']}</div>
            <div class="rec-meta">{profile['desc']}</div>
        </div>
        """, unsafe_allow_html=True)

    st.info("👈 **Use the sidebar on the left** to set your cuisine, location, budget, and delivery preferences, then click **Find Recommended Restaurants** to get your personalized recommendations.")

