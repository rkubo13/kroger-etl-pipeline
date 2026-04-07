"""Main pipeline orchestrator — runs the full ETL end-to-end.

Can be invoked directly (python -m etl.pipeline) or called from
an Airflow task. Each step is a standalone function so Airflow
can retry individual stages independently.
"""

import uuid
from datetime import datetime, timezone

from etl.extractors.csv_extractor import CSVExtractor
from etl.transformers.clean import clean_transactions, clean_products, clean_households
from etl.transformers.enrich import enrich_transactions
from etl.transformers.aggregate import build_basket_summary, build_household_summary
from etl.loaders.warehouse_loader import WarehouseLoader
from config.logging_config import get_logger

logger = get_logger(__name__)


def run_extract(extractor: CSVExtractor) -> dict:
    """Step 1: Extract and validate all raw datasets."""
    return {
        "transactions": extractor.extract("transactions"),
        "products": extractor.extract("products"),
        "households": extractor.extract("households"),
    }


def run_transform(raw: dict) -> dict:
    """Step 2: Clean, enrich, and aggregate."""
    # Clean
    txn_clean = clean_transactions(raw["transactions"])
    prod_clean = clean_products(raw["products"])
    hh_clean = clean_households(raw["households"])

    # Enrich
    txn_enriched = enrich_transactions(txn_clean, prod_clean)

    # Aggregate
    baskets = build_basket_summary(txn_enriched)
    household_summary = build_household_summary(baskets, hh_clean)

    return {
        "transactions_clean": txn_enriched,
        "baskets": baskets,
        "household_summary": household_summary,
    }


def run_load(loader: WarehouseLoader, tables: dict) -> dict:
    """Step 3: Load all tables into the warehouse."""
    results = {}
    for table_name, df in tables.items():
        count = loader.load(df, table_name)
        results[table_name] = count
    return results


def run_pipeline(config_path: str = "config/pipeline_config.yaml") -> dict:
    """Execute the full ETL pipeline."""
    run_id = str(uuid.uuid4())[:8]
    start = datetime.now(timezone.utc)
    logger.info(f"Pipeline run {run_id} started at {start.isoformat()}")

    try:
        extractor = CSVExtractor(config_path)
        loader = WarehouseLoader(config_path)

        raw = run_extract(extractor)
        tables = run_transform(raw)
        load_results = run_load(loader, tables)

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        summary = {
            "run_id": run_id,
            "status": "SUCCESS",
            "elapsed_seconds": elapsed,
            "tables_loaded": load_results,
        }
        logger.info(f"Pipeline run {run_id} completed in {elapsed:.1f}s: {load_results}")
        return summary

    except Exception as e:
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        logger.error(f"Pipeline run {run_id} FAILED after {elapsed:.1f}s: {e}")
        raise


if __name__ == "__main__":
    run_pipeline()
