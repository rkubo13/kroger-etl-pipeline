"""Enrich transactions with business logic and derived fields."""

import pandas as pd

from config.logging_config import get_logger

logger = get_logger(__name__)


def enrich_transactions(transactions: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    """Join transactions with product catalog and add derived columns."""
    logger.info(f"Enriching {len(transactions)} transactions with {len(products)} products")

    product_cols = ["product_id", "department", "category", "brand", "unit_price"]
    df = transactions.merge(
        products[product_cols], on="product_id", how="left", suffixes=("", "_product")
    )

    unmatched = df["department"].isna().sum()
    if unmatched > 0:
        logger.warning(f"{unmatched} transactions had no matching product")

    df["line_total"] = df["quantity"] * df["unit_price"]

    df["transaction_date_dt"] = pd.to_datetime(df["transaction_date"])
    df["day_of_week"] = df["transaction_date_dt"].dt.day_name()
    df["is_weekend"] = df["transaction_date_dt"].dt.dayofweek >= 5

    df["basket_id"] = df["household_id"] + "_" + df["transaction_date"]
    df = df.drop(columns=["transaction_date_dt"])

    logger.info(f"Enrichment complete: {len(df)} rows, {len(df.columns)} columns")
    return df
