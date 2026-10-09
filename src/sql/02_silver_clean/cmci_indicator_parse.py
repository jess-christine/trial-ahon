"""Build the five incremental CMCI Silver pillar tables from Bronze rows."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CMCI_CODE_DIR = Path(__file__).resolve().parents[1] / "01_bronze_ingest"
sys.path.insert(0, str(CMCI_CODE_DIR))

from cmci_common import INDICATORS_BY_PILLAR

CATALOG = os.environ.get("AHON_CATALOG", "ahon")
BRONZE_TABLE = f"{CATALOG}.bronze.cmci_raw_indicator"
BATCH_TABLE = f"{CATALOG}.bronze.cmci_raw_indicator_batch_html"
SILVER_PREFIX = f"{CATALOG}.silver.cmci_"
KEY_COLUMNS = ("psgc_code", "year")


def latest_lgu_year_batch(bronze):
    """Keep the complete most-recent CMCI request batch for each LGU-year."""
    batch_window = Window.partitionBy(*KEY_COLUMNS).orderBy(
        F.col("ingestion_timestamp").desc(),
        F.col("_ingested_at").desc(),
        F.col("batch_id").desc(),
    )
    batches = (
        bronze.select(*KEY_COLUMNS, "batch_id", "ingestion_timestamp", "_ingested_at")
        .distinct()
        .withColumn("_batch_rank", F.row_number().over(batch_window))
        .filter(F.col("_batch_rank") == 1)
        .select(*KEY_COLUMNS, F.col("batch_id").alias("_latest_batch_id"))
    )
    source = bronze.alias("source")
    latest = batches.alias("latest")
    return source.join(
        latest,
        (F.col("source.psgc_code") == F.col("latest.psgc_code"))
        & (F.col("source.year") == F.col("latest.year"))
        & (F.col("source.batch_id") == F.col("latest._latest_batch_id")),
        "inner",
    ).select("source.*")


def build_pillar_frame(latest_rows, indicators: dict[str, str]):
    """Pivot configured indicator labels into the approved pillar columns."""
    values = F.when(
        F.trim(F.col("raw_value")).isin("", "-"), F.lit(None).cast("double")
    ).otherwise(F.expr("try_cast(raw_value AS DOUBLE)"))

    columns = [
        F.max(F.when(F.col("indicator_label") == label, values)).alias(code)
        for label, code in indicators.items()
    ]
    aggregates = [
        F.max("psgc_name").alias("lgu"),
        F.max("cmci_name").alias("cmci_name"),
        F.max("response_hash").alias("source_response_hash"),
        F.max("ingestion_timestamp").alias("source_ingestion_timestamp"),
        F.current_timestamp().alias("silver_processed_timestamp"),
        *columns,
    ]
    return latest_rows.groupBy(*KEY_COLUMNS).agg(*aggregates)


def merge_pillar(spark: SparkSession, table_name: str, frame) -> None:
    """Create a pillar table or merge on its documented PSGC-year grain."""
    if not spark.catalog.tableExists(table_name):
        frame.write.format("delta").mode("error").saveAsTable(table_name)
        return

    existing_columns = set(spark.table(table_name).columns)
    incoming_columns = set(frame.columns)
    if existing_columns != incoming_columns:
        raise ValueError(
            f"Silver schema drift for {table_name}: "
            f"missing={sorted(existing_columns - incoming_columns)}, "
            f"new={sorted(incoming_columns - existing_columns)}"
        )

    target = DeltaTable.forName(spark, table_name)
    condition = " AND ".join(
        f"target.{column} = source.{column}" for column in KEY_COLUMNS
    )
    (
        target.alias("target")
        .merge(frame.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def main() -> None:
    """Refresh observed Silver LGU-years without synthesizing missing rows."""
    spark = SparkSession.builder.getOrCreate()
    batch_metadata = spark.table(BATCH_TABLE)
    complete_batches = batch_metadata.filter(
        (F.size("requested_psgc_codes") == F.col("expected_lgu_count"))
        & (F.size("requested_cmci_names") == F.col("expected_lgu_count"))
        & (F.size("requested_years") == F.col("expected_year_count"))
        & (F.size("requested_indicator_codes") == F.col("expected_indicator_count"))
        & (
            F.col("returned_value_count")
            == F.col("expected_lgu_count")
            * F.col("expected_year_count")
            * F.col("expected_indicator_count")
        )
    ).select("batch_id", "returned_value_count").distinct()
    raw = spark.table(BRONZE_TABLE)
    actual = raw.groupBy("batch_id").count()
    broken_batches = complete_batches.join(actual, "batch_id", "left").filter(
        F.coalesce(F.col("count"), F.lit(0)) != F.col("returned_value_count")
    ).count()
    if broken_batches:
        raise ValueError(
            f"{broken_batches} complete CMCI HTML batches do not reconcile to row Bronze; "
            "run CMCI ingestion recovery before Silver"
        )
    bronze = raw.join(complete_batches.select("batch_id"), "batch_id", "inner")
    expected_labels = [
        label
        for indicators in INDICATORS_BY_PILLAR.values()
        for label in indicators
    ]
    if (
        bronze.filter(
            F.col("indicator_label").isNull()
            | ~F.col("indicator_label").isin(expected_labels)
        )
        .limit(1)
        .count()
    ):
        raise ValueError("Bronze CMCI contains unconfigured indicator labels")
    approved = spark.table(f"{CATALOG}.reference.cmci_lgu_map").filter(
        F.col("is_active") & F.col("reviewed") & (F.col("match_status") == "MATCHED")
    ).select("psgc_code", "cmci_name")
    for key in ("psgc_code", "cmci_name"):
        if approved.groupBy(key).count().filter("count > 1").limit(1).count():
            raise ValueError(f"Reviewed CMCI mapping has duplicate active {key} keys")
    active = spark.table(f"{CATALOG}.reference.lgu_master").filter("is_active").select("psgc_code")
    if approved.join(active, "psgc_code", "left_anti").limit(1).count():
        raise ValueError("Reviewed CMCI mapping contains an inactive or unknown PSGC key")
    if bronze.join(approved, ["psgc_code", "cmci_name"], "left_anti").limit(1).count():
        raise ValueError("CMCI Bronze identity does not agree with the reviewed PSGC mapping")
    latest_rows = latest_lgu_year_batch(bronze)
    if (
        latest_rows.groupBy(*KEY_COLUMNS, "batch_id", "indicator_label")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    ):
        raise ValueError(
            "Latest CMCI batch contains duplicate LGU-year-indicator keys"
        )

    incomplete = latest_rows.groupBy(*KEY_COLUMNS).agg(
        F.countDistinct("indicator_label").alias("indicator_count")
    ).filter(F.col("indicator_count") != len(expected_labels)).count()
    if incomplete:
        raise ValueError(f"{incomplete} CMCI LGU-years lack the approved indicator set")
    expected_rows = latest_rows.select(*KEY_COLUMNS).distinct().count()
    if expected_rows == 0:
        raise ValueError("No complete CMCI LGU-years available; Silver was not changed")
    print(f"CMCI complete input: {expected_rows} LGU-years")
    for pillar_name, indicators in INDICATORS_BY_PILLAR.items():
        table_suffix = pillar_name.lower().replace(" ", "_")
        table_name = SILVER_PREFIX + table_suffix
        frame = build_pillar_frame(latest_rows, indicators)
        merge_pillar(spark, table_name, frame)
        print(f"{table_name}: {expected_rows} LGU-years upserted")



if __name__ == "__main__":
    main()
