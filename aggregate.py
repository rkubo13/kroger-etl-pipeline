"""Aggregate enriched transactions into basket-level and household-level tables.

These rollups are the analytics-ready tables that downstream data scientists
use for segmentation, promotion optimization, and media targeting.
"""

import pandas as pd
from config.logging_config import get_logger

logger = get_logger(__name__)


def build_basket_summary(enriched: pd.DataFrame) -> pd.DataFrame:
    """Aggregate to one row per basket (household + shopping trip).

    Output columns:
        basket_id, household_id, transaction_date, store_id,
        item_count, unique_products, total_spend, avg_item_price,
        departments_visited, category_breadth
    """
    logger.info(f"Building basket summaries from {enriched['basket_id'].nunique()} baskets")

    baskets = enriched.groupby("basket_id").agg(
        household_id=("household_id", "first"),
        transaction_date=("transaction_date", "first"),
        store_id=("store_id", "first"),
        item_count=("quantity", "sum"),
        unique_products=("product_id", "nunique"),
        total_spend=("sales_amount", "sum"),
        avg_item_price=("sales_amount", "mean"),
        departments_visited=("department", "nunique"),
        category_breadth=("category", "nunique"),
    ).reset_index()

    logger.info(f"Built {len(baskets)} basket summaries")
    return baskets


def build_household_summary(
    baskets: pd.DataFrame, households: pd.DataFrame
) -> pd.DataFrame:
    """Roll up baskets to one row per household with behavioral metrics.

    Output columns:
        household_id, loyalty_tier, household_size,
        total_trips, total_spend, avg_basket_size, avg_basket_spend,
        favorite_store, first_visit, last_visit, days_active
    """
    logger.info(f"Building household summaries from {baskets['household_id'].nunique()} households")

    hh_agg = baskets.groupby("household_id").agg(
        total_trips=("basket_id", "count"),
        total_spend=("total_spend", "sum"),
        avg_basket_size=("item_count", "mean"),
        avg_basket_spend=("total_spend", "mean"),
        favorite_store=("store_id", lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else None),
        first_visit=("transaction_date", "min"),
        last_visit=("transaction_date", "max"),
    ).reset_index()

    # Calculate active window in days
    hh_agg["first_visit"] = pd.to_datetime(hh_agg["first_visit"])
    hh_agg["last_visit"] = pd.to_datetime(hh_agg["last_visit"])
    hh_agg["days_active"] = (hh_agg["last_visit"] - hh_agg["first_visit"]).dt.days
    hh_agg["first_visit"] = hh_agg["first_visit"].dt.strftime("%Y-%m-%d")
    hh_agg["last_visit"] = hh_agg["last_visit"].dt.strftime("%Y-%m-%d")

    # Join household dimensions
    hh_summary = hh_agg.merge(
        households[["household_id", "loyalty_tier", "household_size"]],
        on="household_id",
        how="left",
    )

    logger.info(f"Built {len(hh_summary)} household summaries")
    return hh_summary
