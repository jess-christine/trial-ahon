import os

from bs4 import BeautifulSoup
from cmci_common import (
    CMCI_YEARS,
    EXPECTED_INDICATOR_COUNT,
    PORTAL_URL,
    REQUEST_TIMEOUT_SECONDS,
    SOURCE_NAME,
    SOURCE_REF,
    create_http_session,
)
from pyspark.sql import functions as F

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------

CATALOG = os.environ.get("AHON_CATALOG", "ahon")

LGU_MASTER_TABLE = f"{CATALOG}.reference.lgu_master"

CMCI_MAP_TABLE = f"{CATALOG}.reference.cmci_lgu_map"

BRONZE_TABLE = (
    f"{CATALOG}.bronze.cmci_raw_indicator_batch_html"
)

SILVER_TABLES = [
    f"{CATALOG}.silver.cmci_economic_dynamism",
    f"{CATALOG}.silver.cmci_government_efficiency",
    f"{CATALOG}.silver.cmci_infrastructure",
    f"{CATALOG}.silver.cmci_resiliency",
    f"{CATALOG}.silver.cmci_innovation",
]

EXPECTED_YEARS = list(CMCI_YEARS)

EXPECTED_BATCH_SIZE = 10

# ------------------------------------------------------------------
# Spark configuration
# ------------------------------------------------------------------

spark.conf.set(
    "spark.sql.session.timeZone",
    "UTC",
)


# ------------------------------------------------------------------
# Validate required tables
# ------------------------------------------------------------------

required_tables = [
    LGU_MASTER_TABLE,
    CMCI_MAP_TABLE,
    BRONZE_TABLE,
] + SILVER_TABLES

print("CMCI source profiling started")
print()

for table_name in required_tables:
    try:
        column_count = len(
            spark.table(
                table_name
            ).columns
        )

        print(
            "FOUND | "
            + table_name
            + " | Columns: "
            + str(column_count)
        )

    except Exception as error:
        raise RuntimeError(
            "Required table cannot be accessed: "
            + table_name
            + " | "
            + str(error)
        ) from error


# ------------------------------------------------------------------
# Profile live CMCI portal
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
    raise RuntimeError(
        "CMCI portal returned an empty response"
    )

portal_soup = BeautifulSoup(
    portal_html,
    "html.parser",
)

page_title = (
    portal_soup.title.get_text(
        " ",
        strip=True,
    )
    if portal_soup.title is not None
    else "Unknown"
)

lgu_select = portal_soup.find(
    "select",
    attrs={
        "id": "lgu",
    },
)

if lgu_select is None:
    raise RuntimeError(
        "CMCI LGU selector was not found"
    )

cmci_names = []

for option in lgu_select.find_all("option"):
    option_value = option.get(
        "value"
    )

    if option_value is None:
        continue

    option_value = option_value.strip()

    if option_value:
        cmci_names.append(
            option_value
        )

cmci_names = list(
    dict.fromkeys(
        cmci_names
    )
)

year_values = []

for option in portal_soup.find_all("option"):
    option_value = str(
        option.get(
            "value",
            "",
        )
    ).strip()

    option_text = option.get_text(
        " ",
        strip=True,
    )

    for candidate in (
        option_value,
        option_text,
    ):
        if (
            len(candidate) == 4
            and candidate.isdigit()
            and 2000 <= int(candidate) <= 2100
        ):
            year_values.append(
                candidate
            )

available_years = sorted(
    set(
        year_values
    )
)

print()
print("LIVE CMCI SOURCE")

print(
    "Page title: "
    + page_title
)

print(
    "CMCI locality values: "
    + str(len(cmci_names))
)

print(
    "Available years found: "
    + str(available_years)
)

missing_expected_years = sorted(
    set(EXPECTED_YEARS)
    - set(available_years)
)

if missing_expected_years:
    print(
        "WARNING | Expected years not found: "
        + str(missing_expected_years)
    )


# ------------------------------------------------------------------
# Profile PSGC reference
# ------------------------------------------------------------------

active_lgu_df = (
    spark.table(
        LGU_MASTER_TABLE
    )
    .where(
        F.col("is_active") == True
    )
)

active_psgc_count = (
    active_lgu_df
    .select(
        "psgc_code"
    )
    .dropDuplicates()
    .count()
)

duplicate_psgc_count = (
    active_lgu_df
    .groupBy(
        "psgc_code"
    )
    .count()
    .where(
        F.col("count") > 1
    )
    .count()
)

geographic_level_rows = (
    active_lgu_df
    .groupBy(
        "geographic_level"
    )
    .count()
    .orderBy(
        "geographic_level"
    )
    .collect()
)

print()
print("PSGC REFERENCE PROFILE")

print(
    "Active PSGC LGUs: "
    + str(active_psgc_count)
)

print(
    "Duplicate PSGC codes: "
    + str(duplicate_psgc_count)
)

for row in geographic_level_rows:
    print(
        str(row["geographic_level"])
        + ": "
        + str(row["count"])
    )


# ------------------------------------------------------------------
# Profile CMCI mappings
# ------------------------------------------------------------------

mapping_df = spark.table(
    CMCI_MAP_TABLE
)

mapping_total_count = (
    mapping_df.count()
)

status_rows = (
    mapping_df
    .groupBy(
        "match_status"
    )
    .count()
    .orderBy(
        "match_status"
    )
    .collect()
)

method_rows = (
    mapping_df
    .groupBy(
        "match_method"
    )
    .count()
    .orderBy(
        "match_method"
    )
    .collect()
)

approved_mapping_df = (
    mapping_df
    .where(
        F.col("match_status") == "MATCHED"
    )
    .where(
        F.col("reviewed") == True
    )
    .where(
        F.col("is_active") == True
    )
)

approved_mapping_count = (
    approved_mapping_df.count()
)

unmatched_mapping_count = (
    mapping_df
    .where(
        F.col("match_status") == "UNMATCHED"
    )
    .count()
)

ambiguous_mapping_count = (
    mapping_df
    .where(
        F.col("match_status") == "AMBIGUOUS"
    )
    .count()
)

duplicate_active_cmci_count = (
    approved_mapping_df
    .groupBy(
        "cmci_name"
    )
    .count()
    .where(
        F.col("count") > 1
    )
    .count()
)

approved_missing_master_count = (
    approved_mapping_df
    .select(
        "psgc_code"
    )
    .join(
        active_lgu_df.select(
            "psgc_code"
        ),
        on="psgc_code",
        how="left_anti",
    )
    .count()
)

if active_psgc_count == 0:
    raise RuntimeError(
        "No active PSGC LGUs were found in "
        + LGU_MASTER_TABLE
    )

mapping_coverage_percentage = (
    approved_mapping_count
    / active_psgc_count
    * 100
)

print()
print("CMCI MAPPING PROFILE")

print(
    "Total mapping records: "
    + str(mapping_total_count)
)

print(
    "Approved active mappings: "
    + str(approved_mapping_count)
)

print(
    "Unmatched mappings: "
    + str(unmatched_mapping_count)
)

print(
    "Ambiguous mappings: "
    + str(ambiguous_mapping_count)
)

print(
    "Duplicate active CMCI names: "
    + str(duplicate_active_cmci_count)
)

print(
    "Approved PSGC codes missing from master: "
    + str(approved_missing_master_count)
)

print(
    "Approved PSGC coverage: "
    + str(
        round(
            mapping_coverage_percentage,
            2,
        )
    )
    + "%"
)

print()
print("MAPPING STATUS COUNTS")

for row in status_rows:
    print(
        str(row["match_status"])
        + ": "
        + str(row["count"])
    )

print()
print("MAPPING METHOD COUNTS")

for row in method_rows:
    print(
        str(row["match_method"])
        + ": "
        + str(row["count"])
    )


# ------------------------------------------------------------------
# Preview unresolved LGUs
# ------------------------------------------------------------------

unmatched_preview = (
    mapping_df
    .where(
        F.col("match_status") != "MATCHED"
    )
    .select(
        "psgc_code",
        "psgc_name",
        "match_status",
    )
    .orderBy(
        "psgc_code"
    )
    .limit(
        20
    )
    .collect()
)

print()
print("FIRST 20 UNRESOLVED PSGC LGUS")

for row in unmatched_preview:
    print(
        str(row["psgc_code"])
        + " | "
        + str(row["psgc_name"])
        + " | "
        + str(row["match_status"])
    )


# ------------------------------------------------------------------
# Profile Bronze coverage
# ------------------------------------------------------------------

bronze_df = spark.table(
    BRONZE_TABLE
)

required_bronze_columns = {
    "batch_id",
    "requested_psgc_codes",
    "requested_cmci_names",
    "requested_years",
    "requested_indicator_codes",
    "expected_lgu_count",
    "expected_year_count",
    "expected_indicator_count",
    "returned_value_count",
    "response_html",
    "response_hash",
    "ingestion_timestamp",
    "_source_name",
    "_source_ref",
    "_ingested_at",
    "_batch_id",
    "_row_hash",
}

missing_bronze_columns = (
    required_bronze_columns
    - set(bronze_df.columns)
)

if missing_bronze_columns:
    raise RuntimeError(
        "Bronze table is missing required columns: "
        + str(
            sorted(
                missing_bronze_columns
            )
        )
    )

invalid_provenance_count = (
    bronze_df
    .where(
        F.col("_source_name").isNull()
        | (F.trim(F.col("_source_name")) == "")
        | F.col("_source_ref").isNull()
        | (F.trim(F.col("_source_ref")) == "")
        | F.col("_ingested_at").isNull()
        | F.col("_batch_id").isNull()
        | (F.trim(F.col("_batch_id")) == "")
        | F.col("_row_hash").isNull()
        | (
            F.length(
                F.trim(
                    F.col("_row_hash")
                )
            )
            != 64
        )
    )
    .count()
)

unexpected_source_name_count = (
    bronze_df
    .where(
        F.col("_source_name")
        != SOURCE_NAME
    )
    .count()
)

unexpected_source_ref_count = (
    bronze_df
    .where(
        F.col("_source_ref")
        != SOURCE_REF
    )
    .count()
)

if invalid_provenance_count > 0:
    raise RuntimeError(
        "Bronze contains "
        + str(invalid_provenance_count)
        + " rows with incomplete provenance"
    )

if unexpected_source_name_count > 0:
    raise RuntimeError(
        "Bronze contains "
        + str(unexpected_source_name_count)
        + " rows with an unexpected _source_name"
    )

if unexpected_source_ref_count > 0:
    raise RuntimeError(
        "Bronze contains "
        + str(unexpected_source_ref_count)
        + " rows with an unexpected _source_ref"
    )


bronze_total_rows = (
    bronze_df.count()
)

expected_final_batch_size = (
    approved_mapping_count
    % EXPECTED_BATCH_SIZE
)

allowed_batch_sizes = [
    EXPECTED_BATCH_SIZE
]

if expected_final_batch_size > 0:
    allowed_batch_sizes.append(
        expected_final_batch_size
    )

production_bronze_df = (
    bronze_df
    .where(
        F.col("expected_year_count")
        == len(EXPECTED_YEARS)
    )
    .where(
        F.col("expected_indicator_count")
        == EXPECTED_INDICATOR_COUNT
    )
    .where(
        F.col("expected_lgu_count").isin(
            allowed_batch_sizes
        )
    )
)

production_batch_count = (
    production_bronze_df.count()
)

production_sums = (
    production_bronze_df
    .select(
        F.sum(
            "expected_lgu_count"
        ).alias(
            "lgu_slots"
        ),
        F.sum(
            "returned_value_count"
        ).alias(
            "returned_values"
        ),
    )
    .first()
)

production_lgu_slots = (
    production_sums["lgu_slots"]
    or 0
)

production_value_count = (
    production_sums["returned_values"]
    or 0
)

expected_production_value_count = (
    production_lgu_slots
    * len(EXPECTED_YEARS)
    * EXPECTED_INDICATOR_COUNT
)

invalid_returned_value_batch_count = (
    production_bronze_df
    .where(
        F.col("returned_value_count")
        != (
            F.col("expected_lgu_count")
            * F.col("expected_year_count")
            * F.col("expected_indicator_count")
        )
    )
    .count()
)

bronze_psgc_df = (
    production_bronze_df
    .select(
        F.explode(
            "requested_psgc_codes"
        ).alias(
            "psgc_code"
        )
    )
    .select(
        F.trim(
            F.col("psgc_code")
        ).alias(
            "psgc_code"
        )
    )
    .where(
        F.col("psgc_code").isNotNull()
    )
    .where(
        F.col("psgc_code") != ""
    )
)

distinct_bronze_psgc_count = (
    bronze_psgc_df
    .dropDuplicates()
    .count()
)

duplicate_bronze_psgc_count = (
    bronze_psgc_df
    .groupBy(
        "psgc_code"
    )
    .count()
    .where(
        F.col("count") > 1
    )
    .count()
)

unauthorized_bronze_psgc_count = (
    bronze_psgc_df
    .dropDuplicates()
    .join(
        approved_mapping_df.select(
            "psgc_code"
        ),
        on="psgc_code",
        how="left_anti",
    )
    .count()
)

missing_bronze_psgc_count = (
    approved_mapping_df
    .select(
        "psgc_code"
    )
    .join(
        bronze_psgc_df.dropDuplicates(),
        on="psgc_code",
        how="left_anti",
    )
    .count()
)

print()
print("BRONZE COVERAGE PROFILE")

print(
    "All Bronze rows: "
    + str(bronze_total_rows)
)

print(
    "Production batches: "
    + str(production_batch_count)
)

print(
    "Production LGU slots: "
    + str(production_lgu_slots)
)

print(
    "Production values: "
    + str(production_value_count)
)

print(
    "Expected valid batch sizes: "
    + str(allowed_batch_sizes)
)

print(
    "Distinct Bronze PSGC codes: "
    + str(distinct_bronze_psgc_count)
)

print(
    "Duplicate Bronze PSGC assignments: "
    + str(duplicate_bronze_psgc_count)
)

print(
    "Unauthorized Bronze PSGC codes: "
    + str(unauthorized_bronze_psgc_count)
)

print(
    "Approved PSGC codes missing from Bronze: "
    + str(missing_bronze_psgc_count)
)

print(
    "Expected production values: "
    + str(expected_production_value_count)
)

print(
    "Batches with invalid returned-value counts: "
    + str(invalid_returned_value_batch_count)
)

print(
    "Rows with incomplete provenance: "
    + str(invalid_provenance_count)
)

# ------------------------------------------------------------------
# Profile Silver coverage
# ------------------------------------------------------------------

expected_lgu_year_count = (
    approved_mapping_count
    * len(EXPECTED_YEARS)
)

print()
print("SILVER COVERAGE PROFILE")

silver_issues = []

for table_name in SILVER_TABLES:
    silver_df = spark.table(
        table_name
    )

    row_count = (
        silver_df.count()
    )

    unique_key_count = (
        silver_df
        .select(
            "psgc_code",
            "year",
        )
        .dropDuplicates()
        .count()
    )

    distinct_psgc_count = (
        silver_df
        .select(
            "psgc_code"
        )
        .dropDuplicates()
        .count()
    )

    unauthorized_psgc_count = (
        silver_df
        .select(
            "psgc_code"
        )
        .dropDuplicates()
        .join(
            approved_mapping_df.select(
                "psgc_code"
            ),
            on="psgc_code",
            how="left_anti",
        )
        .count()
    )

    distinct_years = sorted(
        [
            row["year"]
            for row in (
                silver_df
                .select(
                    "year"
                )
                .dropDuplicates()
                .collect()
            )
        ]
    )

    print(
        table_name
        + " | Rows: "
        + str(row_count)
        + " | Unique keys: "
        + str(unique_key_count)
        + " | LGUs: "
        + str(distinct_psgc_count)
        + " | Years: "
        + str(len(distinct_years))
        + " | Unauthorized PSGC codes: "
        + str(unauthorized_psgc_count)
    )

    if row_count != expected_lgu_year_count:
        silver_issues.append(
            table_name
            + " has unexpected row count"
        )

    if unique_key_count != expected_lgu_year_count:
        silver_issues.append(
            table_name
            + " has unexpected key coverage"
        )

    if distinct_psgc_count != approved_mapping_count:
        silver_issues.append(
            table_name
            + " has unexpected LGU coverage"
        )

    if distinct_years != EXPECTED_YEARS:
        silver_issues.append(
            table_name
            + " has unexpected year coverage"
        )

    if unauthorized_psgc_count > 0:
        silver_issues.append(
            table_name
            + " contains unauthorized PSGC codes"
        )


# ------------------------------------------------------------------
# Final profile summary
# ------------------------------------------------------------------

print()
print("CMCI SOURCE PROFILE COMPLETE")

print(
    "PSGC reference denominator: "
    + str(active_psgc_count)
)

print(
    "CMCI pipeline denominator: "
    + str(approved_mapping_count)
)

print(
    "Known unmatched LGUs: "
    + str(unmatched_mapping_count)
)

print(
    "Expected LGU-year rows per Silver table: "
    + str(expected_lgu_year_count)
)

if silver_issues:
    print()
    print("PROFILE WARNINGS")

    for issue in silver_issues:
        print(
            "WARNING | "
            + issue
        )

else:
    print(
        "Silver coverage matches the approved CMCI scope"
    )

print()
print(
    "Read-only profile complete. "
    + "No reference, Bronze, or Silver data was changed."
)
