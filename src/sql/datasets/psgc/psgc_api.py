# Databricks notebook source
# ruff: noqa
# Generated from: psgc_api.ipynb
# Converted at: 2026-10-05T20:17:10.840Z
# It uses dbutils and spark which are only available in Databricks environments.

# --- Parameters ---
# Pass secret identifiers as job parameters; the token itself stays in the
# workspace secret store and is never written to notebook parameters or logs.
dbutils.widgets.text("psgc_secret_scope", "", "PSGC token secret scope")
dbutils.widgets.text("psgc_secret_key", "", "PSGC token secret key")
dbutils.widgets.text("psgc_api_base_url", "", "PSGC API base URL")
dbutils.widgets.text("psgc_periods", "", "Comma-separated PSGC publication periods")

import os
import time
import uuid

import requests
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, sha2, to_json

API_BASE = dbutils.widgets.get("psgc_api_base_url").strip().rstrip("/")
PERIODS = [
    period.strip()
    for period in dbutils.widgets.get("psgc_periods").split(",")
    if period.strip()
]
CATALOG = os.environ.get("AHON_CATALOG", "ahon")
BRONZE_TABLE = f"{CATALOG}.bronze.psgc"

secret_scope = dbutils.widgets.get("psgc_secret_scope").strip()
secret_key = dbutils.widgets.get("psgc_secret_key").strip()
api_token = dbutils.secrets.get(scope=secret_scope, key=secret_key) if secret_scope and secret_key else ""

if not api_token:
    raise RuntimeError("Configure the PSGC API token through Databricks Secrets")
if not API_BASE or not PERIODS:
    raise RuntimeError("Configure a PSGC API base URL and publication periods")
print(f"Periods     : {', '.join(PERIODS)}")
print(f"Bronze table: {BRONZE_TABLE}")

# --- Fetch all records from the PSGC API ---
all_records = []

for period in PERIODS:
    url = f"{API_BASE}/{period}/all?token={api_token}"
    page = 1
    while url:
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as error:
            status = (
                error.response.status_code
                if error.response is not None
                else "network error"
            )
            raise RuntimeError(
                f"PSGC request failed for period {period}, page {page} ({status})"
            ) from None
        except ValueError:
            raise RuntimeError(
                f"PSGC returned invalid JSON for period {period}, page {page}"
            ) from None
        results = data.get("results", [])
        all_records.extend(results)
        print(
            f"  {period:<12} page {page:>3}  got {len(results):>5} records  "
            f"(total: {len(all_records)})"
        )
        url = data.get("next")
        page += 1
        time.sleep(0.2)  # gentle rate-limit between pages

print(f"\nTotal records fetched across all periods: {len(all_records)}")

# --- Create bronze table and write PSGC raw records ---
# Expects all_records and API_BASE from ../extract/psgc_parameters (run first).

# 1. Ensure the bronze table exists
spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
    psgc_code             STRING,
    area_name             STRING,
    correspondence_code   STRING,
    geographic_level      STRING,
    region_code           STRING,
    province_code         STRING,
    municipality_code     STRING,
    barangay_code         STRING,
    old_name              STRING,
    city_class            STRING,
    income_classification STRING,
    urban_rural           STRING,
    island_region         STRING,
    status                STRING,
    version               STRING,
    populations_json      STRING,
    _source_name          STRING,
    _source_ref           STRING,
    _ingested_at          TIMESTAMP,
    _batch_id             STRING,
    _row_hash             STRING
)
COMMENT 'Raw PSGC API records fetched across multiple periods.\n'
        'populations_json holds the nested populations array as a JSON string.\n'
        '_source_name/_source_ref identify the API origin; _ingested_at and _batch_id\n'
        'tag the ingestion run; _row_hash is a SHA-256 of all raw source columns.'
"""
)
print(f"  ✓ Table ensured: {BRONZE_TABLE}")

# 2. Build a Spark DataFrame from the fetched Python dicts.
if not all_records:
    raise RuntimeError(
        "all_records is empty. Check that the PSGC API token is set and "
        "the fetch cell in ../extract/psgc_parameters completed successfully."
    )

raw_df = spark.createDataFrame(all_records)

# 3. Transform API column names to the bronze schema and add provenance.
raw_cols = [
    "psgc_code",
    "area_name",
    "correspondence_code",
    "geographic_level",
    "region_code",
    "province_code",
    "municipality_code",
    "barangay_code",
    "old_name",
    "city_class",
    "income_classification",
    "urban_rural",
    "island_region",
    "status",
    "version",
    "populations_json",
]

batch_id = str(uuid.uuid4())

source_df = (
    raw_df.withColumn("populations_json", to_json(col("populations")))
    .select(
        col("code").alias("psgc_code"),
        col("area_name"),
        col("correspondence_code"),
        col("geographic_level"),
        col("reg").cast("string").alias("region_code"),
        col("prv").cast("string").alias("province_code"),
        col("mun").cast("string").alias("municipality_code"),
        col("bgy").cast("string").alias("barangay_code"),
        col("old_name"),
        col("city_class"),
        col("income_classification"),
        col("urban_rural"),
        col("island_region"),
        col("status"),
        col("version"),
        col("populations_json"),
    )
    .withColumn("_source_name", lit("psgc"))
    .withColumn("_source_ref", lit(API_BASE))
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_batch_id", lit(batch_id))
    .withColumn("_row_hash", sha2(concat_ws("|", *[col(c) for c in raw_cols]), 256))
)

# 4. MERGE into bronze for idempotency (dedup on _row_hash).
source_df.createOrReplaceTempView("source_view")
row_count = source_df.count()

merge_sql = f"""
MERGE INTO {BRONZE_TABLE} AS t
USING source_view AS s
ON t._row_hash = s._row_hash
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

spark.sql(merge_sql)

print(f"Rows to write : {row_count}")
print(f"Batch ID      : {batch_id}")
