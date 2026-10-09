# Databricks notebook source
"""Load the PSA census population CSV into the bronze Delta table."""

from __future__ import annotations

import csv
import os
import uuid
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, sha2
from pyspark.sql.types import StringType, StructField, StructType

# COMMAND ----------

# Settings: change here if the team renames anything
CATALOG = os.environ.get("AHON_CATALOG", "ahon")
VOLUME_ROOT = Path(os.environ.get("AHON_SOURCE_VOLUME", f"/Volumes/{CATALOG}/reference/source"))
DATASET_NAME = "psa_population"
SOURCE_PATH = VOLUME_ROOT / DATASET_NAME / "2024_population_urban.csv"
TABLE_NAME = f"{CATALOG}.bronze.psa_population_raw"
CENSUS_YEAR = "2024"
MERGE_KEYS = ["geographic_location", "census_year"]
RAW_COLS = [
    "geographic_location",
    "total_population",
    "urban_population",
    "percent_urban",
    "census_year",
]
PROVENANCE_COLS = [
    "_source_name",
    "_source_ref",
    "_ingested_at",
    "_batch_id",
    "_row_hash",
]
SCHEMA = StructType([StructField(c, StringType(), True) for c in RAW_COLS])

CREATE_TABLE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        geographic_location     STRING,
        total_population        STRING,
        urban_population        STRING,
        percent_urban           STRING,
        census_year             STRING,
        _source_name            STRING,
        _source_ref             STRING,
        _ingested_at            TIMESTAMP,
        _batch_id               STRING,
        _row_hash               STRING
    ) USING DELTA
"""

# COMMAND ----------


def read_source_rows(
    path: Path, census_year: str
) -> list[tuple[str, str, str, str, str]]:
    """Read the extract CSV and build full hierarchical geographic paths.

    The PXWeb CSV uses dot-indentation for hierarchy (2 dots per level). Leaf
    names like "San Isidro" appear in multiple provinces, so the full path is
    needed for unique merge keys.
    """
    rows: list[tuple[str, str, str, str, str]] = []
    parents: list[tuple[int, str]] = []
    with path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            loc = row["Geographic Location"]
            name = loc.lstrip(".")
            level = (len(loc) - len(name)) // 2
            while parents and parents[-1][0] >= level:
                parents.pop()
            parents.append((level, name))
            rows.append(
                (
                    " > ".join(n for _, n in parents),
                    row["Total Population"],
                    row["Urban Population"],
                    row["Percent Urban"],
                    census_year,
                )
            )
    return rows


def add_provenance(df: DataFrame, batch_id: str) -> DataFrame:
    """Add provenance columns and a SHA-256 row hash."""
    return df.withColumns(
        {
            "_source_name": lit("PSA PXWeb API"),
            "_source_ref": lit(str(SOURCE_PATH)),
            "_ingested_at": current_timestamp(),
            "_batch_id": lit(batch_id),
            "_row_hash": sha2(concat_ws("|", *[col(c) for c in RAW_COLS]), 256),
        }
    )


def merge_into_bronze(spark: SparkSession, df: DataFrame) -> None:
    """Create the bronze table if missing, then upsert on the merge keys."""
    spark.sql(CREATE_TABLE_SQL)
    condition = " AND ".join(f"target.{key} <=> source.{key}" for key in MERGE_KEYS)
    (
        DeltaTable.forName(spark, TABLE_NAME)
        .alias("target")
        .merge(df.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def main() -> None:
    """Read the extracted CSV, add provenance, and merge into bronze."""
    spark = SparkSession.builder.getOrCreate()

    rows = read_source_rows(SOURCE_PATH, CENSUS_YEAR)
    batch_id = str(uuid.uuid4())
    df = add_provenance(spark.createDataFrame(rows, SCHEMA), batch_id)
    merge_into_bronze(spark, df)

    print(f"Batch ID   : {batch_id}")
    print(f"Merged {len(rows)} rows into {TABLE_NAME}")
    print(f"Merge keys : {MERGE_KEYS}")
    print(f"Columns    : {RAW_COLS + PROVENANCE_COLS}")


# COMMAND ----------

if __name__ == "__main__":
    main()
