"""Clean raw data: deduplicate, handle nulls, normalize formats."""

import pandas as pd

from config.logging_config import get_logger

logger = get_logger(__name__)


def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Clean transaction data for downstream use."""
    initial_count = len(df)

    df = df.drop_duplicates(subset=["transaction_id"], keep="first")
    logger.info(f"Dedup: {initial_count} -> {len(df)} rows")

    df = df.dropna(subset=["transaction_id", "household_id", "product_id"])

    df["transaction_date"] = pd.to_datetime(df["transaction_date"], format="mixed", errors="coerce")
    df = df.dropna(subset=["transaction_date"])
    df["transaction_date"] = df["transaction_date"].dt.strftime("%Y-%m-%d")

    df["quantity"] = df["quantity"].clip(lower=0)
    df["sales_amount"] = df["sales_amount"].clip(lower=0.0)

    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].str.strip()

    logger.info(f"Cleaning complete: {initial_count} -> {len(df)} rows")
    return df.reset_index(drop=True)


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize product catalog data."""
    df = df.drop_duplicates(subset=["product_id"], keep="first")
    df = df.dropna(subset=["product_id", "product_name"])

    for col in ["department", "category", "brand"]:
        if col in df.columns:
            df[col] = df[col].str.strip().str.upper()

    df["unit_price"] = df["unit_price"].clip(lower=0.0)
    return df.reset_index(drop=True)


def clean_households(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize household dimension data."""
    df = df.drop_duplicates(subset=["household_id"], keep="first")
    df = df.dropna(subset=["household_id"])

    df["loyalty_tier"] = df["loyalty_tier"].str.strip().str.upper()
    df["household_size"] = df["household_size"].clip(lower=1)
    return df.reset_index(drop=True)
