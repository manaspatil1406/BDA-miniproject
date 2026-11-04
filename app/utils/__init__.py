"""
FoodieAnalytics Utilities Module
Provides cached data loading and recommendation engine utilities.
"""

from .data_loader import (
    load_spark_query,
    load_all_spark_queries,
    load_clustered_restaurants,
    load_streaming_summaries,
    load_streaming_orders,
    get_dataset_kpis,
    load_css
)

from .recommendation_engine import (
    recommend_restaurants,
    get_filter_options,
    get_cluster_profiles
)

__all__ = [
    "load_spark_query",
    "load_all_spark_queries",
    "load_clustered_restaurants",
    "load_streaming_summaries",
    "load_streaming_orders",
    "get_dataset_kpis",
    "load_css",
    "recommend_restaurants",
    "get_filter_options",
    "get_cluster_profiles"
]

