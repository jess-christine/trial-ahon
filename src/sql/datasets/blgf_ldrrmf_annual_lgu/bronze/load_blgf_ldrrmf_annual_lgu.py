# Load LDRRMF Excel files into bronze
# Reads the 7 BLGF LDRRMF Excel files from the source volume and writes ONE bronze table with all years stacked.
# Suggested repo path: src/sql/datasets/blgf_ldrrmf_annual_lgu/bronze/load_blgf_ldrrmf_annual_lgu.py
# What it does:
#   1. Finds the data sheet and the header rows in each file (the layout differs between years).
#   2. Maps columns by header name, not by position, and renames them to the bronze column names.
#   3. Checks each file's row count against the profile, and stops before writing if any file differs.
#   4. Adds the provenance columns from the naming standard and overwrites the bronze table.
# Bronze keeps the values as received. Cleaning (fiscal_year, PSGC, flags) happens in silver.
# Run it as a file (Databricks job) or paste it into a notebook cell.

import os
import uuid
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

spark = SparkSession.builder.getOrCreate()

# Settings: change here if the team renames anything
CATALOG = os.environ.get("AHON_CATALOG", "ahon")
SOURCE_NAME = "blgf_ldrrmf_annual_lgu"
SOURCE_DIR = Path(os.environ.get("AHON_SOURCE_VOLUME", f"/Volumes/{CATALOG}/reference/source")) / SOURCE_NAME
TARGET_TABLE = f"{CATALOG}.bronze.{SOURCE_NAME}"

# LGU rows per year from the bronze profile, used to check each file after reading
EXPECTED_ROWS = {2018: 1513, 2019: 1566, 2020: 1612, 2021: 1637, 2022: 1683, 2023: 1707, 2024: 1716}

# Source header -> bronze column name (the dev table's names, kept for now; column naming rules are still open)
TEXT_COLUMNS = {
    "REGION": "region",
    "PROVINCE": "province",
    "LGU NAME": "lgu_name",
    "LGU TYPE": "lgu_type",
    "LGU CODE": "lgu_type",  # FY2021 labels this column "LGU CODE", but it holds the LGU type (Province, City, ...)
}
# The amount columns have a two-level header: a group name over a sub-header
AMOUNT_GROUPS = {
    "70% LDRRMF": "70pct_ldrrmf",
    "30% QUICK RESPONSE FUND": "30pct_quick_response_fund",
    "TOTAL": "total",
}
AMOUNT_PARTS = {
    "Budget Appropriation": "budget_appropriation",
    "Expenditures": "expenditures",
}

# Final column order (the source order differs between years, so this fixes it)
TEXT_NAMES = ["region", "province", "lgu_name", "lgu_type"]
AMOUNT_NAMES = [f"{g}_{p}" for g in AMOUNT_GROUPS.values() for p in AMOUNT_PARTS.values()]
RAW_COLUMNS = TEXT_NAMES + AMOUNT_NAMES


def clean_header(value):
    # Header cell as text, with empty cells as ""
    return "" if pd.isna(value) else str(value).strip()


def read_lgu_file(file):
    # Returns one pandas table with the bronze column names, one row per LGU line of the file

    # The data sheet is the one that is not called Metadata (its name and position change by year)
    excel = pd.ExcelFile(file)
    data_sheets = [s for s in excel.sheet_names if s.strip().lower() != "metadata"]
    if len(data_sheets) != 1:
        raise ValueError(f"{file.name}: expected 1 data sheet, found {excel.sheet_names}")
    raw = excel.parse(data_sheets[0], header=None)  # header=None: every row stays data, read by position

    # The first header row is the one that holds "REGION"; the sub-header row is right below it
    header_rows = [i for i in range(len(raw)) if "REGION" in [clean_header(v) for v in raw.iloc[i]]]
    if len(header_rows) != 1:
        raise ValueError(f"{file.name}: could not find the header row")
    first = header_rows[0]
    group_row = raw.iloc[first].ffill()  # group names sit over merged cells, so fill them across
    part_row = raw.iloc[first + 1]

    # Map every source column to its bronze name by header text
    names = {}
    for col in raw.columns:
        group = clean_header(group_row[col])
        part = clean_header(part_row[col])
        if group in TEXT_COLUMNS:
            names[col] = TEXT_COLUMNS[group]
        elif group in AMOUNT_GROUPS and part in AMOUNT_PARTS:
            names[col] = f"{AMOUNT_GROUPS[group]}_{AMOUNT_PARTS[part]}"

    # Stop if any expected column is missing or a column is mapped twice
    if sorted(names.values()) != sorted(RAW_COLUMNS):
        raise ValueError(f"{file.name}: unexpected columns {sorted(names.values())}")

    # Data starts two rows below the first header row
    data = raw.iloc[first + 2 :][list(names.keys())].rename(columns=names)[RAW_COLUMNS]

    # Text stays text; amounts become numbers (an amount that is not a number raises an error, on purpose)
    for name in TEXT_NAMES:
        data[name] = data[name].map(lambda v: None if pd.isna(v) else str(v))
    for name in AMOUNT_NAMES:
        data[name] = pd.to_numeric(data[name], errors="raise").astype("float64")  # always float, as Spark expects

    data["_source_ref"] = str(file)  # where the row came from; silver reads the fiscal year from this
    return data


# Read every file and check its row count before anything is written
tables = []
for file in sorted(SOURCE_DIR.glob("*.xlsx")):
    year = int(file.name[2:6])  # digits after "FY" in the file name
    table = read_lgu_file(file)
    print(f"{file.name}: {len(table)} rows (profile says {EXPECTED_ROWS[year]})")
    if len(table) != EXPECTED_ROWS[year]:
        raise ValueError(f"{file.name}: row count differs from the profile, nothing was written")
    tables.append(table)

all_rows = pd.concat(tables, ignore_index=True)

# Convert to Spark with explicit types, so empty cells become nulls instead of guessed types
schema = StructType(
    [StructField(c, StringType(), True) for c in TEXT_NAMES]
    + [StructField(c, DoubleType(), True) for c in AMOUNT_NAMES]
    + [StructField("_source_ref", StringType(), True)]
)
records = all_rows.astype(object).where(all_rows.notna(), None).values.tolist()
df = spark.createDataFrame(records, schema)

# Provenance columns from the naming standard
batch_id = str(uuid.uuid4())  # one id for this whole run
df = (
    df.withColumn("_source_name", F.lit(SOURCE_NAME))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_batch_id", F.lit(batch_id))
    # SHA-256 over the raw source columns only (provenance columns left out)
    .withColumn(
        "_row_hash",
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in RAW_COLUMNS]), 256),
    )
)

# Overwrite: each run rebuilds the table from the files in the volume
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)

# Show the result by year, to compare with the profile
print(f"Wrote {df.count()} rows to {TARGET_TABLE}")
spark.table(TARGET_TABLE).groupBy("_source_ref").count().orderBy("_source_ref").show(truncate=False)
