"""End-to-end integration test: generate data → run pipeline → verify warehouse."""

import pytest
import sys
from pathlib import Path

import duckdb
import pandas as pd

from scripts.generate_data import generate_products, generate_households, generate_transactions
from etl.pipeline import run_pipeline


@pytest.fixture
def integration_env(tmp_path):
    """Set up a complete pipeline environment with synthetic data."""
    # Create directory structure
    raw = tmp_path / "data" / "raw"
    processed = tmp_path / "data" / "processed"
    warehouse = tmp_path / "data" / "warehouse"
    raw.mkdir(parents=True)
    processed.mkdir(parents=True)
    warehouse.mkdir(parents=True)

    # Generate synthetic data
    products = generate_products(50)
    households = generate_households(100)
    transactions = generate_transactions(products, households, n=1000)

    products.to_csv(raw / "products.csv", index=False)
    households.to_csv(raw / "households.csv", index=False)
    transactions.to_csv(raw / "transactions.csv", index=False)

    # Write config pointing to tmp dirs
    config_content = f"""
pipeline:
  name: integration_test
  version: "1.0.0"
paths:
  raw_data: "{raw}"
  processed_data: "{processed}"
  warehouse_db: "{warehouse}/grocery.duckdb"
schemas:
  transactions:
    required_columns:
      - transaction_id
      - household_id
      - product_id
      - store_id
      - transaction_date
      - quantity
      - sales_amount
    types:
      transaction_id: str
      household_id: str
      product_id: str
      store_id: str
      transaction_date: str
      quantity: int
      sales_amount: float
  products:
    required_columns:
      - product_id
      - product_name
      - department
      - category
      - brand
      - unit_price
    types:
      product_id: str
      product_name: str
      department: str
      category: str
      brand: str
      unit_price: float
  households:
    required_columns:
      - household_id
      - loyalty_tier
      - household_size
      - zip_code
    types:
      household_id: str
      loyalty_tier: str
      household_size: int
      zip_code: str
quality:
  max_null_pct: 0.05
  max_duplicate_pct: 0.01
  min_row_count: 10
"""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(config_content)

    return {
        "config_path": str(config_path),
        "warehouse_path": str(warehouse / "grocery.duckdb"),
    }


def test_full_pipeline_produces_all_tables(integration_env):
    """The pipeline should create 3 tables in the warehouse."""
    result = run_pipeline(integration_env["config_path"])

    assert result["status"] == "SUCCESS"
    assert "transactions_clean" in result["tables_loaded"]
    assert "baskets" in result["tables_loaded"]
    assert "household_summary" in result["tables_loaded"]


def test_warehouse_tables_have_expected_columns(integration_env):
    """Verify the schema of output tables."""
    run_pipeline(integration_env["config_path"])

    con = duckdb.connect(integration_env["warehouse_path"], read_only=True)

    # transactions_clean should have enrichment columns
    txn_cols = {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'transactions_clean'"
        ).fetchall()
    }
    assert "basket_id" in txn_cols
    assert "day_of_week" in txn_cols
    assert "department" in txn_cols

    # baskets should have aggregation columns
    basket_cols = {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'baskets'"
        ).fetchall()
    }
    assert "total_spend" in basket_cols
    assert "unique_products" in basket_cols

    # household_summary should have behavioral metrics
    hh_cols = {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'household_summary'"
        ).fetchall()
    }
    assert "total_trips" in hh_cols
    assert "loyalty_tier" in hh_cols

    con.close()


def test_no_duplicates_in_output(integration_env):
    """Cleaned transactions should have no duplicate transaction_ids."""
    run_pipeline(integration_env["config_path"])

    con = duckdb.connect(integration_env["warehouse_path"], read_only=True)
    dupes = con.execute(
        "SELECT transaction_id, COUNT(*) as cnt "
        "FROM transactions_clean GROUP BY transaction_id HAVING cnt > 1"
    ).fetchall()
    con.close()

    assert len(dupes) == 0, f"Found {len(dupes)} duplicate transaction_ids"


def test_pipeline_is_idempotent(integration_env):
    """Running the pipeline twice should produce identical results."""
    result1 = run_pipeline(integration_env["config_path"])
    result2 = run_pipeline(integration_env["config_path"])

    assert result1["tables_loaded"] == result2["tables_loaded"]
