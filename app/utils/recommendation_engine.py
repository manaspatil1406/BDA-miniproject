"""
FoodieAnalytics - Recommendation Engine Module
Replicates and extends the Apache Spark MLlib recommendation engine
developed in Stage 8, providing instantaneous interactive scoring,
multi-criteria filtering, and K-Means cluster profiling.
"""

from typing import Optional, Dict, Any, List, Tuple
import pandas as pd
import numpy as np


def get_cluster_profiles() -> Dict[int, Dict[str, str]]:
    """
    Returns descriptive business profiles for each of the 5 clusters
    discovered by the K-Means algorithm in Stage 8.
    """
    return {
        0: {
            "name": "Budget-Friendly Delivery Gems",
            "desc": "Affordable eateries and quick-service joints with high online delivery adoption and accessible pricing (₹150 - ₹400).",
            "color": "#10B981"
        },
        1: {
            "name": "Fine Dining & Premium Lounges",
            "desc": "Upscale restaurants with high cost for two (> ₹1,500), extensive seating, table booking, and curated international menus.",
            "color": "#8B5CF6"
        },
        2: {
            "name": "Popular Casual & Family Dine-Out",
            "desc": "Mid-tier restaurants (₹500 - ₹900) offering North Indian, Chinese, and South Indian fare with high customer footfall.",
            "color": "#3B82F6"
        },
        3: {
            "name": "Trending Cafes & Youth Hangouts",
            "desc": "High-rated contemporary cafes, bakeries, and beverage spots favored for casual socialization.",
            "color": "#F59E0B"
        },
        4: {
            "name": "Iconic Nightlife & Microbreweries",
            "desc": "Top-tier breweries and pubs with massive vote volumes (5,000+ votes), exceptional ratings (4.5+), and vibrant social dining.",
            "color": "#E23744"
        }
    }


def get_filter_options(df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    """
    Extract clean, sorted lists of unique Bangalore localities and common cuisines
    for Streamlit filter components.
    """
    locations = sorted([loc for loc in df["location_clean"].dropna().unique() if loc != "Unknown"])
    
    # Extract distinct individual cuisines
    all_cuisines = set()
    for item in df["cuisines"].dropna():
        for c in str(item).split(","):
            c_clean = c.strip()
            if c_clean and len(c_clean) > 1:
                all_cuisines.add(c_clean)
                
    # Sort cuisines by frequency or alphabetical
    sorted_cuisines = sorted(list(all_cuisines))
    
    # Place most popular cuisines at top of list
    popular_top = ["North Indian", "Chinese", "South Indian", "Fast Food", "Biryani", "Continental", "Italian", "Cafe", "Desserts", "Beverages"]
    ordered_cuisines = [c for c in popular_top if c in sorted_cuisines] + [c for c in sorted_cuisines if c not in popular_top]
    
    return locations, ordered_cuisines


def recommend_restaurants(
    df: pd.DataFrame,
    cuisine: Optional[str] = None,
    location: Optional[str] = None,
    budget: Optional[float] = None,
    min_rating: Optional[float] = None,
    delivery: str = "any",
    top_n: int = 10
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Evaluates multi-attribute restaurant recommendations using the exact weighted
    scoring formula implemented in Stage 8:
    
    Score = (0.40 * Cuisine_Score + 0.25 * Rating_Score + 0.15 * Budget_Score + 0.10 * Delivery_Score + 0.10 * Location_Score) * 100
    
    Parameters:
    - df: Pre-loaded clustered restaurant DataFrame.
    - cuisine: Filter string or None (substring match against cuisines).
    - location: Filter string or None (exact or substring match on locality).
    - budget: Maximum cost for two (INR) or None.
    - min_rating: Minimum threshold rating (0.0 to 5.0) or None.
    - delivery: 'any', 'yes', or 'no'.
    - top_n: Number of ranked recommendations to return.
    
    Returns:
    - Ranked DataFrame with calculated component scores and final recommendation score.
    - Summary metadata dictionary (total available, matched, applied filters).
    """
    filtered = df.copy()
    
    # 1. Apply Cuisine Filter (Case-insensitive substring match)
    if cuisine and cuisine.strip() and cuisine.strip().lower() != "all cuisines":
        c_clean = cuisine.strip().lower()
        filtered = filtered[
            filtered["cuisines"].astype(str).str.lower().str.contains(c_clean, regex=False, na=False)
        ]
        
    # 2. Apply Location Filter
    if location and location.strip() and location.strip().lower() != "all bangalore":
        loc_clean = location.strip().lower()
        filtered = filtered[
            filtered["location_clean"].astype(str).str.lower().str.contains(loc_clean, regex=False, na=False)
        ]
        
    # 3. Apply Budget Filter (cost_for_two_num <= budget)
    if budget is not None and budget > 0:
        filtered = filtered[
            filtered["cost_for_two_num"].notna() & (filtered["cost_for_two_num"] <= budget)
        ]
        
    # 4. Apply Minimum Rating Filter (rating_num >= min_rating)
    if min_rating is not None and min_rating > 0:
        filtered = filtered[
            filtered["rating_num"].notna() & (filtered["rating_num"] >= min_rating)
        ]
        
    # 5. Apply Delivery Filter
    if delivery.lower() == "yes":
        filtered = filtered[filtered["online_order_bool"] == True]
    elif delivery.lower() == "no":
        filtered = filtered[filtered["online_order_bool"] == False]
    # For "any", all records (True, False, None) are permitted.
    
    matching_count = len(filtered)
    
    metadata = {
        "total_catalog": len(df),
        "matching_count": matching_count,
        "cuisine_filter": cuisine,
        "location_filter": location,
        "budget_filter": budget,
        "rating_filter": min_rating,
        "delivery_filter": delivery
    }
    
    if matching_count == 0:
        return pd.DataFrame(), metadata
        
    # Calculate Scoring Components strictly adhering to Stage 8 specification:
    # A. Cuisine Score = 1.0 (satisfies preferred cuisine)
    filtered["cuisine_score"] = 1.0
    
    # B. Rating Score: normalized between 0 and 1 (rating / 5.0)
    filtered["rating_score"] = (filtered["rating_num"] / 5.0).clip(lower=0.0, upper=1.0).fillna(0.0)
    
    # C. Budget Compatibility Score: max(0.0, 1.0 - cost / budget)
    if budget is not None and budget > 0:
        filtered["budget_score"] = np.maximum(
            0.0,
            1.0 - (filtered["cost_for_two_num"] / budget)
        ).fillna(0.5)
    else:
        filtered["budget_score"] = 1.0
        
    # D. Delivery Score = 1.0 (satisfies requirement)
    filtered["delivery_score"] = 1.0
    
    # E. Location Score = 1.0 (satisfies preferred locality)
    filtered["location_score"] = 1.0
    
    # Calculate Final Weighted Score
    # Formula: (0.40 * C + 0.25 * R + 0.15 * B + 0.10 * D + 0.10 * L) * 100
    filtered["recommendation_score"] = (
        0.40 * filtered["cuisine_score"] +
        0.25 * filtered["rating_score"] +
        0.15 * filtered["budget_score"] +
        0.10 * filtered["delivery_score"] +
        0.10 * filtered["location_score"]
    ) * 100.0
    
    filtered["recommendation_score"] = filtered["recommendation_score"].round(2)
    
    # Rank candidates by: recommendation_score DESC, rating_num DESC, votes_num DESC
    ranked = filtered.sort_values(
        by=["recommendation_score", "rating_num", "votes_num"],
        ascending=[False, False, False]
    ).head(top_n)
    
    # Map cluster descriptions
    cluster_meta = get_cluster_profiles()
    ranked["cluster_name"] = ranked["cluster_id"].map(
        lambda cid: cluster_meta.get(int(cid), {}).get("name", f"Cluster {cid}") if pd.notna(cid) else "Standard"
    )
    
    return ranked, metadata

