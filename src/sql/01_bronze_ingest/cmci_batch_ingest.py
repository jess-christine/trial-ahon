import argparse
import hashlib
import json
import math
import os
import time
import uuid
from datetime import datetime, timezone

from bs4 import BeautifulSoup
from cmci_common import (
    CMCI_YEARS,
    EXPECTED_INDICATOR_COUNT,
    EXPECTED_INDICATOR_LABELS,
    PORTAL_URL,
    PROCESS_URL,
    REQUEST_TIMEOUT_SECONDS,
    REQUESTED_INDICATOR_CODES,
    SOURCE_NAME,
    SOURCE_REF,
    create_http_session,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

spark = SparkSession.builder.getOrCreate()

# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------

CATALOG = os.environ.get("AHON_CATALOG", "ahon")
CMCI_MAP_TABLE = f"{CATALOG}.reference.cmci_lgu_map"

TARGET_TABLE = f"{CATALOG}.bronze.cmci_raw_indicator_batch_html"

REQUEST_DELAY_SECONDS = 1.0

YEARS = list(CMCI_YEARS)

# ------------------------------------------------------------------
# RUN ARGUMENTS
# ------------------------------------------------------------------

parser = argparse.ArgumentParser()

parser.add_argument(
    "--run-mode",
    choices=[
        "test",
        "full",
    ],
    default="full",
)

parser.add_argument(
    "--batch-size",
    type=int,
    default=10,
)

parser.add_argument(
    "--test-lgu-limit",
    type=int,
    default=5,
)

parser.add_argument(
    "--refresh-existing",
    action="store_true",
)

run_arguments, unknown_arguments = (
    parser.parse_known_args()
)

RUN_MODE = run_arguments.run_mode
BATCH_SIZE = run_arguments.batch_size
TEST_LGU_LIMIT = run_arguments.test_lgu_limit
REFRESH_EXISTING = run_arguments.refresh_existing

# ------------------------------------------------------------------
# VALIDATE RUN CONFIG
# ------------------------------------------------------------------

if BATCH_SIZE < 1:
    raise ValueError(
        "batch-size must be at least 1"
    )

if TEST_LGU_LIMIT < 1:
    raise ValueError(
        "test-lgu-limit must be at least 1"
    )

if not YEARS:
    raise ValueError(
        "YEARS cannot be empty"
    )

if len(REQUESTED_INDICATOR_CODES) != 35:
    raise RuntimeError(
        "Expected 35 indicator codes"
    )

if REFRESH_EXISTING:
    raise RuntimeError(
        "refresh-existing is not currently supported because "
        + "it can create duplicate PSGC coverage in Bronze"
    )

# ------------------------------------------------------------------
# BRONZE SCHEMA
# ------------------------------------------------------------------

BRONZE_SCHEMA = StructType(
    [
        StructField(
            "batch_id",
            StringType(),
            nullable=False,
        ),
        StructField(
            "requested_psgc_codes",
            ArrayType(
                StringType(),
                containsNull=False,
            ),
            nullable=False,
        ),
        StructField(
            "requested_cmci_names",
            ArrayType(
                StringType(),
                containsNull=False,
            ),
            nullable=False,
        ),
        StructField(
            "requested_years",
            ArrayType(
                StringType(),
                containsNull=False,
            ),
            nullable=False,
        ),
        StructField(
            "requested_indicator_codes",
            ArrayType(
                StringType(),
                containsNull=False,
            ),
            nullable=False,
        ),
        StructField(
            "expected_lgu_count",
            IntegerType(),
            nullable=False,
        ),
        StructField(
            "expected_year_count",
            IntegerType(),
            nullable=False,
        ),
        StructField(
            "expected_indicator_count",
            IntegerType(),
            nullable=False,
        ),
        StructField(
            "returned_value_count",
            IntegerType(),
            nullable=False,
        ),
        StructField(
            "response_html",
            StringType(),
            nullable=False,
        ),
        StructField(
            "response_hash",
            StringType(),
            nullable=False,
        ),
        StructField(
            "ingestion_timestamp",
            TimestampType(),
            nullable=False,
        ),
        StructField(
            "_source_name",
            StringType(),
            nullable=False,
        ),
        StructField(
            "_source_ref",
            StringType(),
            nullable=False,
        ),
        StructField(
            "_ingested_at",
            TimestampType(),
            nullable=False,
        ),
        StructField(
            "_batch_id",
            StringType(),
            nullable=False,
        ),
        StructField(
            "_row_hash",
            StringType(),
            nullable=False,
        ),
    ]
)

# ------------------------------------------------------------------
# SPARK CONFIG
# ------------------------------------------------------------------

spark.conf.set(
    "spark.sql.session.timeZone",
    "UTC",
)

# ------------------------------------------------------------------
# VALIDATE REQUIRED TABLES
# ------------------------------------------------------------------

required_tables = [
    CMCI_MAP_TABLE,
    TARGET_TABLE,
]

for table_name in required_tables:
    try:
        spark.table(
            table_name
        ).limit(1).collect()

        print(
            "Required table found: "
            + table_name
        )

    except RuntimeError as error:
        raise RuntimeError(
            "Required table cannot be accessed: "
            + table_name
            + " | "
            + str(error)
        ) from error

# ------------------------------------------------------------------
# VALIDATE BRONZE TARGET SCHEMA
# ------------------------------------------------------------------

required_target_columns = {
    field.name
    for field in BRONZE_SCHEMA.fields
}

actual_target_columns = set(
    spark.table(
        TARGET_TABLE
    ).columns
)

missing_target_columns = (
    required_target_columns
    - actual_target_columns
)

if missing_target_columns:
    raise RuntimeError(
        "Bronze target table is missing required columns: "
        + str(
            sorted(
                missing_target_columns
            )
        )
        + ". Update the Delta table schema before ingestion."
    )

# ------------------------------------------------------------------
# LOAD APPROVED MAPPINGS
# ------------------------------------------------------------------

approved_mapping_df = (
    spark.table(
        CMCI_MAP_TABLE
    )
    .where(
        F.col("match_status") == "MATCHED"
    )
    .where(
        F.col("reviewed") == True
    )
    .where(
        F.col("is_active") == True
    )
    .select(
        F.trim("psgc_code").alias(
            "psgc_code"
        ),
        F.trim("psgc_name").alias(
            "psgc_name"
        ),
        F.trim("cmci_name").alias(
            "cmci_name"
        ),
    )
    .where(
        F.col("psgc_code").isNotNull()
    )
    .where(
        F.col("cmci_name").isNotNull()
    )
    .where(
        F.col("psgc_code") != ""
    )
    .where(
        F.col("cmci_name") != ""
    )
    .dropDuplicates(
        [
            "psgc_code",
        ]
    )
    .orderBy(
        "psgc_code"
    )
)

duplicate_cmci_name_count = (
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

if duplicate_cmci_name_count > 0:
    raise RuntimeError(
        "Approved mappings contain duplicate "
        + "active CMCI names"
    )

approved_mapping_count = (
    approved_mapping_df.count()
)

EXPECTED_APPROVED_MAPPING_COUNT = 1626

if (
    approved_mapping_count
    != EXPECTED_APPROVED_MAPPING_COUNT
):
    raise RuntimeError(
        "Expected "
        + str(EXPECTED_APPROVED_MAPPING_COUNT)
        + " approved active CMCI mappings, found "
        + str(approved_mapping_count)
    )

existing_bronze_psgc_df = (
    spark.table(
        TARGET_TABLE
    )
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
    .dropDuplicates()
)

existing_bronze_psgc_count = (
    existing_bronze_psgc_df.count()
)

unauthorized_bronze_psgc_count = (
    existing_bronze_psgc_df
    .join(
        approved_mapping_df.select(
            "psgc_code"
        ),
        on="psgc_code",
        how="left_anti",
    )
    .count()
)

if unauthorized_bronze_psgc_count > 0:
    raise RuntimeError(
        "Bronze contains "
        + str(unauthorized_bronze_psgc_count)
        + " PSGC codes outside the approved CMCI scope"
    )

pending_mapping_df = (
    approved_mapping_df
    .join(
        existing_bronze_psgc_df,
        on="psgc_code",
        how="left_anti",
    )
    .orderBy(
        "psgc_code"
    )
)

pending_mapping_count = (
    pending_mapping_df.count()
)

print(
    "Approved CMCI mappings: "
    + str(approved_mapping_count)
)

print(
    "LGUs already represented in Bronze: "
    + str(existing_bronze_psgc_count)
)

print(
    "Approved LGUs pending ingestion: "
    + str(pending_mapping_count)
)

def main():
    # executable workflow

    if pending_mapping_count == 0:
        print(
            "No pending approved CMCI LGUs. "
            + "No Bronze rows were written."
        )
        return


if __name__ == "__main__":
    main()

if RUN_MODE == "test":
    selected_mapping_df = (
        pending_mapping_df.limit(
            TEST_LGU_LIMIT
        )
    )
else:
    selected_mapping_df = (
        pending_mapping_df
    )

LGU_MAPPINGS = [
    {
        "psgc_code": row["psgc_code"],
        "psgc_name": row["psgc_name"],
        "cmci_name": row["cmci_name"],
    }
    for row in selected_mapping_df.collect()
]

if not LGU_MAPPINGS:
    raise RuntimeError(
        "No approved LGU mappings were selected"
    )

# ------------------------------------------------------------------
# CREATE BATCHES
# ------------------------------------------------------------------

LGU_BATCHES = []

for batch_start in range(
    0,
    len(LGU_MAPPINGS),
    BATCH_SIZE,
):
    LGU_BATCHES.append(
        LGU_MAPPINGS[
            batch_start:
            batch_start + BATCH_SIZE
        ]
    )

expected_batch_count = math.ceil(
    len(LGU_MAPPINGS)
    / BATCH_SIZE
)

if len(LGU_BATCHES) != expected_batch_count:
    raise RuntimeError(
        "Batch-count validation failed"
    )

# ------------------------------------------------------------------
# CREATE DETERMINISTIC BATCH ID
# ------------------------------------------------------------------

def create_batch_id(
    batch_mappings,
):
    identity_parts = []

    for mapping in batch_mappings:
        identity_parts.append(
            mapping["psgc_code"]
        )

    identity_parts.extend(
        YEARS
    )

    identity_parts.extend(
        REQUESTED_INDICATOR_CODES
    )

    canonical_identity = "|".join(
        identity_parts
    )

    return hashlib.sha256(
        canonical_identity.encode(
            "utf-8"
        )
    ).hexdigest()

# ------------------------------------------------------------------
# CREATE BATCH PAYLOAD
# ------------------------------------------------------------------

def create_batch_payload(
    batch_mappings,
):
    payload = []

    for mapping in batch_mappings:
        payload.append(
            (
                "chk-lgu[]",
                mapping["cmci_name"],
            )
        )

    for year in YEARS:
        payload.append(
            (
                "chk-year[]",
                year,
            )
        )

    for indicator_code in (
        REQUESTED_INDICATOR_CODES
    ):
        payload.append(
            (
                "chk-indicators[]",
                indicator_code,
            )
        )

    return payload

# ------------------------------------------------------------------
# PARSE AND VALIDATE BATCH RESPONSE
# ------------------------------------------------------------------

def parse_batch_response(
    response_html,
    batch_mappings,
):
    soup = BeautifulSoup(
        response_html,
        "html.parser",
    )

    if soup.title is None:
        raise ValueError(
            "CMCI response has no page title"
        )

    page_title = soup.title.get_text(
        " ",
        strip=True,
    )

    if "Data Portal Results" not in page_title:
        raise ValueError(
            "Unexpected page title: "
            + page_title
        )

    tables = soup.find_all(
        "table"
    )

    if len(tables) != EXPECTED_INDICATOR_COUNT:
        raise ValueError(
            "Expected "
            + str(EXPECTED_INDICATOR_COUNT)
            + " indicator tables, found "
            + str(len(tables))
        )

    requested_names = {
        mapping["cmci_name"]
        for mapping in batch_mappings
    }

    parsed_values = {}
    returned_indicators = set()

    for table in tables:
        heading = table.find_previous(
            "h2"
        )

        if heading is None:
            raise ValueError(
                "Indicator table has no heading"
            )

        indicator_label = heading.get_text(
            " ",
            strip=True,
        )

        if indicator_label not in (
            EXPECTED_INDICATOR_LABELS
        ):
            raise ValueError(
                "Unexpected indicator table: "
                + indicator_label
            )

        if indicator_label in returned_indicators:
            raise ValueError(
                "Duplicate indicator table: "
                + indicator_label
            )

        returned_indicators.add(
            indicator_label
        )

        rows = []

        for table_row in table.find_all("tr"):
            values = [
                cell.get_text(
                    " ",
                    strip=True,
                )
                for cell in table_row.find_all(
                    (
                        "th",
                        "td",
                    )
                )
            ]

            if values:
                rows.append(
                    values
                )

        if len(rows) != len(batch_mappings) + 1:
            raise ValueError(
                "Unexpected row count for "
                + indicator_label
            )

        header = rows[0]

        if header[0] != "Province / LGU":
            raise ValueError(
                "Unexpected first header for "
                + indicator_label
            )

        returned_years = header[1:]

        if returned_years != YEARS:
            raise ValueError(
                "Year columns do not match request for "
                + indicator_label
                + " | Returned: "
                + str(returned_years)
            )

        returned_names = set()

        for row in rows[1:]:
            if len(row) != len(YEARS) + 1:
                raise ValueError(
                    "Unexpected value count for "
                    + indicator_label
                    + " | "
                    + str(row)
                )

            cmci_name = row[0].strip()

            if cmci_name in returned_names:
                raise ValueError(
                    "Duplicate LGU row for "
                    + indicator_label
                    + " | "
                    + cmci_name
                )

            returned_names.add(
                cmci_name
            )

            for year_number, year in enumerate(
                YEARS,
                start=1,
            ):
                indicator_value = row[
                    year_number
                ].strip()

                if indicator_value not in (
                    "",
                    "-",
                ):
                    try:
                        float(
                            indicator_value
                        )

                    except ValueError as error:
                        raise ValueError(
                            "Non-numeric value: "
                            + indicator_label
                            + " | "
                            + cmci_name
                            + " | "
                            + year
                            + " | "
                            + indicator_value
                        ) from error

                parsed_values[
                    (
                        indicator_label,
                        cmci_name,
                        year,
                    )
                ] = indicator_value

        if returned_names != requested_names:
            missing_names = sorted(
                requested_names
                - returned_names
            )

            unexpected_names = sorted(
                returned_names
                - requested_names
            )

            raise ValueError(
                "Returned LGUs do not match request for "
                + indicator_label
                + " | Missing: "
                + str(missing_names)
                + " | Unexpected: "
                + str(unexpected_names)
            )

    if returned_indicators != EXPECTED_INDICATOR_LABELS:
        missing_indicators = sorted(
            EXPECTED_INDICATOR_LABELS
            - returned_indicators
        )

        unexpected_indicators = sorted(
            returned_indicators
            - EXPECTED_INDICATOR_LABELS
        )

        raise ValueError(
            "Returned indicator set does not match request. "
            + "Missing: "
            + str(missing_indicators)
            + " | Unexpected: "
            + str(unexpected_indicators)
        )

    expected_value_count = (
        len(batch_mappings)
        * len(YEARS)
        * EXPECTED_INDICATOR_COUNT
    )

    if len(parsed_values) != expected_value_count:
        raise ValueError(
            "Expected "
            + str(expected_value_count)
            + " values, parsed "
            + str(len(parsed_values))
        )

    return parsed_values

# ------------------------------------------------------------------
# CALCULATE STABLE RESPONSE HASH
# ------------------------------------------------------------------

def calculate_batch_hash(
    parsed_values,
):
    canonical_parts = []

    for key in sorted(
        parsed_values.keys()
    ):
        indicator_label = key[0]
        cmci_name = key[1]
        year = key[2]

        canonical_parts.append(
            indicator_label
            + "|"
            + cmci_name
            + "|"
            + year
            + "="
            + parsed_values[key]
        )

    canonical_result = "\n".join(
        canonical_parts
    )

    return hashlib.sha256(
        canonical_result.encode(
            "utf-8"
        )
    ).hexdigest()

# ------------------------------------------------------------------
# LOAD EXISTING BATCH STATE
# ------------------------------------------------------------------

existing_batch_rows = (
    spark.table(
        TARGET_TABLE
    )
    .select(
        "batch_id",
        "response_hash",
        "ingestion_timestamp",
    )
    .orderBy(
        F.col(
            "ingestion_timestamp"
        ).desc()
    )
    .collect()
)

existing_batch_hashes = {}

for existing_row in existing_batch_rows:
    batch_id = existing_row[
        "batch_id"
    ]

    if batch_id not in existing_batch_hashes:
        existing_batch_hashes[
            batch_id
        ] = existing_row[
            "response_hash"
        ]

# ------------------------------------------------------------------
# STARTUP SUMMARY
# ------------------------------------------------------------------

print("CMCI batch ingestion started")

print(
    "Run mode: "
    + RUN_MODE
)

print(
    "LGUs selected: "
    + str(len(LGU_MAPPINGS))
)

print(
    "Years selected: "
    + str(YEARS)
)

print(
    "Indicators selected: "
    + str(EXPECTED_INDICATOR_COUNT)
)

print(
    "Batches expected: "
    + str(len(LGU_BATCHES))
)

INGESTION_RUN_ID = str(
    uuid.uuid4()
)

print(
    "Ingestion run ID: "
    + INGESTION_RUN_ID
)

# ------------------------------------------------------------------
# EXECUTE BATCHES
# ------------------------------------------------------------------

session = create_http_session()

successful_batch_count = 0
unchanged_batch_count = 0
written_batch_count = 0
failed_batches = []

try:
    portal_response = session.get(
        PORTAL_URL,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    portal_response.raise_for_status()

    if not portal_response.text.strip():
        raise RuntimeError(
            "CMCI portal returned an empty page"
        )

    print(
        "CMCI portal reachable | Status: "
        + str(portal_response.status_code)
    )

    for batch_number, batch_mappings in enumerate(
        LGU_BATCHES,
        start=1,
    ):
        batch_id = create_batch_id(
            batch_mappings
        )

        if (
            batch_id in existing_batch_hashes
            and not REFRESH_EXISTING
        ):
            print(
                "SKIPPED | Batch "
                + str(batch_number)
                + " of "
                + str(len(LGU_BATCHES))
                + " | Already saved"
            )

            successful_batch_count += 1
            unchanged_batch_count += 1
            continue

        try:
            payload = create_batch_payload(
                batch_mappings
            )

            response = session.post(
                PROCESS_URL,
                data=payload,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

            response.raise_for_status()

            response_html = response.text.strip()

            if not response_html:
                raise ValueError(
                    "CMCI returned an empty response"
                )

            parsed_values = parse_batch_response(
                response_html=response_html,
                batch_mappings=batch_mappings,
            )

            response_hash = calculate_batch_hash(
                parsed_values
            )

            previous_hash = existing_batch_hashes.get(
                batch_id
            )

            if previous_hash == response_hash:
                print(
                    "UNCHANGED | Batch "
                    + str(batch_number)
                    + " of "
                    + str(len(LGU_BATCHES))
                )

                successful_batch_count += 1
                unchanged_batch_count += 1
                continue

            ingested_at = datetime.now(
                timezone.utc
            )

            raw_source_values = {
                "batch_id": batch_id,
                "requested_psgc_codes": [
                    mapping["psgc_code"]
                    for mapping in batch_mappings
                ],
                "requested_cmci_names": [
                    mapping["cmci_name"]
                    for mapping in batch_mappings
                ],
                "requested_years": list(
                    YEARS
                ),
                "requested_indicator_codes": list(
                    REQUESTED_INDICATOR_CODES
                ),
                "expected_lgu_count": len(
                    batch_mappings
                ),
                "expected_year_count": len(
                    YEARS
                ),
                "expected_indicator_count": (
                    EXPECTED_INDICATOR_COUNT
                ),
                "returned_value_count": len(
                    parsed_values
                ),
                "response_html": response_html,
                "response_hash": response_hash,
            }

            row_hash_input = json.dumps(
                raw_source_values,
                sort_keys=True,
                ensure_ascii=False,
                separators=(
                    ",",
                    ":",
                ),
            )

            row_hash = hashlib.sha256(
                row_hash_input.encode(
                    "utf-8"
                )
            ).hexdigest()

            batch_record = {
                **raw_source_values,
                "ingestion_timestamp": ingested_at,
                "_source_name": SOURCE_NAME,
                "_source_ref": SOURCE_REF,
                "_ingested_at": ingested_at,
                "_batch_id": INGESTION_RUN_ID,
                "_row_hash": row_hash,
            }

            batch_df = spark.createDataFrame(
                [
                    batch_record,
                ],
                schema=BRONZE_SCHEMA,
            )

            (
                batch_df.write
                .format("delta")
                .mode("append")
                .saveAsTable(TARGET_TABLE)
            )

            existing_batch_hashes[
                batch_id
            ] = response_hash

            successful_batch_count += 1
            written_batch_count += 1

            print(
                "WRITTEN | Batch "
                + str(batch_number)
                + " of "
                + str(len(LGU_BATCHES))
                + " | LGUs: "
                + str(len(batch_mappings))
                + " | Values: "
                + str(len(parsed_values))
            )

        except (ValueError, RuntimeError) as error:
            failed_batches.append(
                {
                    "batch_number": batch_number,
                    "batch_id": batch_id,
                    "error": str(error),
                }
            )

            print(
                "FAILED | Batch "
                + str(batch_number)
                + " of "
                + str(len(LGU_BATCHES))
                + " | "
                + str(error)
            )

            print(
                "Stopping after the first failed batch "
                + "to preserve stable pending-batch boundaries"
            )

            break

        time.sleep(
            REQUEST_DELAY_SECONDS
        )

finally:
    session.close()

# ------------------------------------------------------------------
# VALIDATE BATCH COUNTS
# ------------------------------------------------------------------

processed_batch_count = (
    successful_batch_count
    + len(failed_batches)
)

if not failed_batches:
    if processed_batch_count != len(LGU_BATCHES):
        raise RuntimeError(
            "Batch-count validation failed. Expected "
            + str(len(LGU_BATCHES))
            + ", processed "
            + str(processed_batch_count)
        )
else:
    print(
        "Batch loop stopped after the first failure. "
        + "Completed batches were preserved."
    )

# ------------------------------------------------------------------
# FINAL SUMMARY
# ------------------------------------------------------------------

print()
print("CMCI BATCH INGESTION COMPLETE")

print(
    "Total batches: "
    + str(len(LGU_BATCHES))
)

print(
    "Successful batches: "
    + str(successful_batch_count)
)

print(
    "Existing or unchanged batches: "
    + str(unchanged_batch_count)
)

print(
    "New or changed batches written: "
    + str(written_batch_count)
)

print(
    "Failed batches: "
    + str(len(failed_batches))
)

# ------------------------------------------------------------------
# REPORT FAILED BATCHES
# ------------------------------------------------------------------

if failed_batches:
    print()
    print("FAILED BATCH DETAILS")

    for failure in failed_batches:
        print(
            "Batch "
            + str(failure["batch_number"])
            + " | "
            + failure["batch_id"]
            + " | "
            + failure["error"]
        )

    raise RuntimeError(
        str(len(failed_batches))
        + " CMCI batch or batches failed. "
        + "Successful batches were preserved and "
        + "will be skipped on the next run."
    )

print()
print(
    "All requested batches completed successfully"
)
