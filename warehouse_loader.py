"""Load transformed DataFrames into DuckDB analytical warehouse.

Key production patterns:
    - Idempotent writes (DROP + CREATE, safe to re-run)
    - Row count verification after load
    - Structured logging of every load operation
"""

from pathlib import Path

import duckdb
import pandas as pd
import yaml

from config.logging_config import get_logger

logger = get_logger(__name__)


class WarehouseLoader:
    """Manages loading DataFrames into a local DuckDB warehouse."""

    def __init__(self, config_path: str = "config/pipeline_config.yaml"):
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        self.db_path = Path(config["paths"]["warehouse_db"])
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def load(self, df: pd.DataFrame, table_name: str) -> int:
        """Load a DataFrame into the warehouse as a table.

        Uses CREATE OR REPLACE for idempotency — the entire table is
        replaced each run. For incremental loads in production, you'd
        use merge/upsert logic keyed on a primary key.

        Args:
            df: The DataFrame to load
            table_name: Destination table name in DuckDB

        Returns:
            Number of rows loaded

        Raises:
            RuntimeError: If post-load row count doesn't match input
        """
        logger.info(f"Loading {len(df)} rows into '{table_name}'")

        con = duckdb.connect(str(self.db_path))
        try:
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
            result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            loaded_count = result[0]

            if loaded_count != len(df):
                raise RuntimeError(
                    f"Row count mismatch for '{table_name}': "
                    f"expected {len(df)}, got {loaded_count}"
                )

            logger.info(
                f"Successfully loaded {loaded_count} rows into '{table_name}'",
                extra={"row_count": loaded_count},
            )
            return loaded_count
        finally:
            con.close()

    def verify(self, table_name: str) -> dict:
        """Return basic stats about a loaded table for monitoring."""
        con = duckdb.connect(str(self.db_path), read_only=True)
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            columns = con.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
            ).fetchall()
            return {
                "table": table_name,
                "row_count": count,
                "columns": [c[0] for c in columns],
            }
        finally:
            con.close()
