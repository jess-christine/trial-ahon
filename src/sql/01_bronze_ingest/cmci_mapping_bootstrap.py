import os
from datetime import datetime, timezone

from bs4 import BeautifulSoup
from cmci_common import (
    PORTAL_URL,
    REQUEST_TIMEOUT_SECONDS,
    create_http_session,
)
from cmci_mapping_update import (
    normalize_candidate_name,
    split_cmci_name_suffix,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

spark = SparkSession.builder.getOrCreate()

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------

CATALOG = os.environ.get("AHON_CATALOG", "ahon")

LGU_MASTER_TABLE = f"{CATALOG}.reference.lgu_master"
CMCI_MAP_TABLE = f"{CATALOG}.reference.cmci_lgu_map"

EXPECTED_LGU_COUNT = 1642
EXPECTED_CMCI_NAME_COUNT = 1634
EXPECTED_MATCHED_COUNT = 1263
EXPECTED_UNMATCHED_COUNT = 379

# ------------------------------------------------------------------
# Spark configuration
# ------------------------------------------------------------------

spark.conf.set("spark.sql.session.timeZone", "UTC")

print("CMCI mapping bootstrap started")
print("Target: " + CMCI_MAP_TABLE)

# ------------------------------------------------------------------
# Validate required tables
# ------------------------------------------------------------------

for table_name in [LGU_MASTER_TABLE, CMCI_MAP_TABLE]:
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(
            "Required table does not exist: " + table_name
        )

print("Required tables found")

# ------------------------------------------------------------------
# Load active LGUs
# ------------------------------------------------------------------

active_lgu_df = (
    spark.table(LGU_MASTER_TABLE)
    .where("is_active = true")
)

active_lgu_count = active_lgu_df.count()

if active_lgu_count != EXPECTED_LGU_COUNT:
    raise RuntimeError(
        "Expected " + str(EXPECTED_LGU_COUNT)
        + " active LGUs, found " + str(active_lgu_count)
    )

print("Active LGUs: " + str(active_lgu_count))

# ------------------------------------------------------------------
# Fetch live CMCI locality names
# ------------------------------------------------------------------

session = create_http_session()

try:
    portal_response = session.get(
        PORTAL_URL,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    portal_response.raise_for_status()
    portal_html = portal_response.text.strip()
finally:
    session.close()

if not portal_html:
    raise RuntimeError("CMCI portal returned an empty response")

portal_soup = BeautifulSoup(portal_html, "html.parser")

lgu_select = portal_soup.find(
    "select",
    attrs={"id": "lgu"},
)

if lgu_select is None:
    raise RuntimeError("CMCI LGU selector was not found")

cmci_names = []

for option in lgu_select.find_all("option"):
    option_value = option.get("value")
    if option_value is None:
        continue
    option_value = option_value.strip()
    if option_value:
        cmci_names.append(option_value)

cmci_names = list(dict.fromkeys(cmci_names))

if len(cmci_names) != EXPECTED_CMCI_NAME_COUNT:
    raise RuntimeError(
        "Expected " + str(EXPECTED_CMCI_NAME_COUNT)
        + " CMCI locality names, found " + str(len(cmci_names))
    )

print("CMCI locality names: " + str(len(cmci_names)))

# ------------------------------------------------------------------
# Build suffix-less CMCI name lookup
# ------------------------------------------------------------------

cmci_exact_lookup = {}

for cmci_name in cmci_names:
    base_name, suffix = split_cmci_name_suffix(cmci_name)
    if suffix is not None:
        continue
    normalized = normalize_candidate_name(base_name)
    if normalized in cmci_exact_lookup:
        raise RuntimeError(
            "Duplicate normalized suffix-less CMCI name: "
            + normalized
        )
    cmci_exact_lookup[normalized] = cmci_name

print("Suffix-less CMCI names: " + str(len(cmci_exact_lookup)))

# ------------------------------------------------------------------
# Match PSGC LGUs to CMCI names
# ------------------------------------------------------------------

lgu_rows = (
    active_lgu_df
    .select("psgc_code", "lgu_name")
    .orderBy("psgc_code")
    .collect()
)

used_cmci_names = set()
mapping_records = []

for row in lgu_rows:
    psgc_code = row["psgc_code"]
    lgu_name = row["lgu_name"]
    normalized = normalize_candidate_name(lgu_name)

    cmci_name = cmci_exact_lookup.get(normalized)

    if cmci_name is not None and cmci_name not in used_cmci_names:
        used_cmci_names.add(cmci_name)
        mapping_records.append({
            "psgc_code": psgc_code,
            "psgc_name": lgu_name,
            "cmci_name": cmci_name,
            "match_status": "MATCHED",
            "match_method": "EXACT_NAME",
            "reviewed": True,
            "is_active": True,
        })
    else:
        mapping_records.append({
            "psgc_code": psgc_code,
            "psgc_name": lgu_name,
            "cmci_name": None,
            "match_status": "UNMATCHED",
            "match_method": None,
            "reviewed": False,
            "is_active": False,
        })

matched_count = sum(
    1 for r in mapping_records
    if r["match_status"] == "MATCHED"
)
unmatched_count = sum(
    1 for r in mapping_records
    if r["match_status"] == "UNMATCHED"
)

print("Matched: " + str(matched_count))
print("Unmatched: " + str(unmatched_count))

if matched_count != EXPECTED_MATCHED_COUNT:
    raise RuntimeError(
        "Expected " + str(EXPECTED_MATCHED_COUNT)
        + " matched, found " + str(matched_count)
    )

if unmatched_count != EXPECTED_UNMATCHED_COUNT:
    raise RuntimeError(
        "Expected " + str(EXPECTED_UNMATCHED_COUNT)
        + " unmatched, found " + str(unmatched_count)
    )

# ------------------------------------------------------------------
# Validate target table is empty
# ------------------------------------------------------------------

existing_count = spark.table(CMCI_MAP_TABLE).count()

if existing_count != 0:
    raise RuntimeError(
        "Target table is not empty: " + str(existing_count)
        + " rows. Bootstrap requires an empty table."
    )

# ------------------------------------------------------------------
# Write to cmci_lgu_map
# ------------------------------------------------------------------

now = datetime.now(timezone.utc)

for record in mapping_records:
    record["created_timestamp"] = now
    record["updated_timestamp"] = now

MAPPING_SCHEMA = StructType([
    StructField("psgc_code", StringType(), nullable=False),
    StructField("psgc_name", StringType(), nullable=False),
    StructField("cmci_name", StringType(), nullable=True),
    StructField("match_status", StringType(), nullable=False),
    StructField("match_method", StringType(), nullable=True),
    StructField("reviewed", BooleanType(), nullable=False),
    StructField("is_active", BooleanType(), nullable=False),
    StructField("created_timestamp", TimestampType(), nullable=False),
    StructField("updated_timestamp", TimestampType(), nullable=False),
])

mapping_df = spark.createDataFrame(mapping_records, schema=MAPPING_SCHEMA)

mapping_df.write.mode("overwrite").saveAsTable(CMCI_MAP_TABLE)

# ------------------------------------------------------------------
# Validate written data
# ------------------------------------------------------------------

written_count = spark.table(CMCI_MAP_TABLE).count()

if written_count != EXPECTED_LGU_COUNT:
    raise RuntimeError(
        "Expected " + str(EXPECTED_LGU_COUNT)
        + " rows after bootstrap, found " + str(written_count)
    )

print()
print("CMCI MAPPING BOOTSTRAP COMPLETE")
print("Total rows written: " + str(written_count))
print("Matched: " + str(matched_count))
print("Unmatched: " + str(unmatched_count))
