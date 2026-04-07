"""Extract and validate raw CSV data against configured schemas."""

import os
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from config.logging_config import get_logger

logger = get_logger(__name__)


class SchemaValidationError(Exception):
    """Raised when raw data fails schema validation."""


class DataQualityError(Exception):
    """Raised when data fails quality thresholds."""


class CSVExtractor:
    """Extracts CSV files with schema validation and quality checks."""

    def __init__(self, config_path: str = "config/pipeline_config.yaml"):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        self.raw_path = Path(self.config["paths"]["raw_data"])
        self.quality = self.config["quality"]

    def extract(self, dataset_name: str) -> pd.DataFrame:
        file_path = self.raw_path / f"{dataset_name}.csv"
        logger.info(f"Extracting {dataset_name} from {file_path}")

        if not file_path.exists():
            raise FileNotFoundError(f"Raw file not found: {file_path}")

        df = pd.read_csv(file_path, dtype=str)
        logger.info(f"Read {len(df)} rows from {dataset_name}")

        schema = self.config["schemas"].get(dataset_name)
        if schema is None:
            raise SchemaValidationError(f"No schema defined for '{dataset_name}'")

        self._validate_columns(df, schema, dataset_name)
        df = self._cast_types(df, schema)
        self._check_quality(df, schema, dataset_name)

        logger.info(f"Extraction complete for {dataset_name}: {len(df)} valid rows")
        return df

    def _validate_columns(self, df, schema, name):
        required = set(schema["required_columns"])
        missing = required - set(df.columns)
        if missing:
            raise SchemaValidationError(f"[{name}] Missing required columns: {missing}")

    def _cast_types(self, df, schema):
        type_map = {"int": "int64", "float": "float64", "str": "object"}
        for col, dtype in schema["types"].items():
            if col in df.columns:
                target = type_map.get(dtype, dtype)
                try:
                    df[col] = df[col].astype(target)
                except (ValueError, TypeError) as e:
                    raise SchemaValidationError(f"Cannot cast '{col}' to {dtype}: {e}")
        return df

    def _check_quality(self, df, schema, name):
        if len(df) < self.quality["min_row_count"]:
            raise DataQualityError(
                f"[{name}] Row count {len(df)} below minimum {self.quality['min_row_count']}"
            )
        for col in schema["required_columns"]:
            null_pct = df[col].isna().mean()
            if null_pct > self.quality["max_null_pct"]:
                raise DataQualityError(
                    f"[{name}] Column '{col}' has {null_pct:.1%} nulls (max: {self.quality['max_null_pct']:.1%})"
                )
        pk_col = schema["required_columns"][0]
        dup_pct = df[pk_col].duplicated().mean()
        if dup_pct > self.quality["max_duplicate_pct"]:
            raise DataQualityError(
                f"[{name}] Column '{pk_col}' has {dup_pct:.1%} duplicates (max: {self.quality['max_duplicate_pct']:.1%})"
            )
