"""Read-only quality checks for every Bronze table currently in the pipeline."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Literal

PROVENANCE_COLUMNS = (
    "_source_name",
    "_source_ref",
    "_ingested_at",
    "_batch_id",
    "_row_hash",
)


@dataclass(frozen=True)
class Check:
    name: str
    severity: Literal["BLOCKING", "WARNING"]
    predicate: str | None = None
    key_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class Dataset:
    table_name: str
    source_name: str
    source_columns: tuple[str, ...]
    checks: tuple[Check, ...]
    coverage_columns: tuple[str, ...] = ()


DATASETS = (
    Dataset(
        "ahon.bronze.blgf_ldrrmf_annual_lgu",
        "blgf_ldrrmf_annual_lgu",
        (
            "region", "province", "lgu_name", "lgu_type",
            "70pct_ldrrmf_budget_appropriation", "70pct_ldrrmf_expenditures",
            "30pct_quick_response_fund_budget_appropriation",
            "30pct_quick_response_fund_expenditures",
            "total_budget_appropriation", "total_expenditures",
        ),
        (
            Check("required_lgu_fields", "WARNING", "region IS NULL OR trim(region) = '' OR province IS NULL OR lgu_name IS NULL OR trim(lgu_name) = '' OR lgu_type IS NULL"),
            Check("source_file_year", "WARNING", "_source_ref IS NULL OR _source_ref NOT RLIKE 'FY[0-9]{4}-LDRRMF-by-LGU[.]xlsx$'"),
            Check("missing_amounts", "WARNING", "`70pct_ldrrmf_budget_appropriation` IS NULL OR `70pct_ldrrmf_expenditures` IS NULL OR `30pct_quick_response_fund_budget_appropriation` IS NULL OR `30pct_quick_response_fund_expenditures` IS NULL OR total_budget_appropriation IS NULL OR total_expenditures IS NULL"),
            Check("zero_total_appropriation_review", "WARNING", "total_budget_appropriation = 0"),
            Check("allowed_lgu_type", "WARNING", "lgu_type NOT IN ('Province', 'City', 'Municipality')"),
            Check("nonnegative_amounts", "WARNING", "`70pct_ldrrmf_budget_appropriation` < 0 OR `70pct_ldrrmf_expenditures` < 0 OR `30pct_quick_response_fund_budget_appropriation` < 0 OR `30pct_quick_response_fund_expenditures` < 0 OR total_budget_appropriation < 0 OR total_expenditures < 0"),
            Check("expenditure_above_appropriation", "WARNING", "`70pct_ldrrmf_expenditures` > `70pct_ldrrmf_budget_appropriation` OR `30pct_quick_response_fund_expenditures` > `30pct_quick_response_fund_budget_appropriation` OR total_expenditures > total_budget_appropriation"),
            Check("appropriation_totals_match_parts", "WARNING", "abs(total_budget_appropriation - (`70pct_ldrrmf_budget_appropriation` + `30pct_quick_response_fund_budget_appropriation`)) > 0.01"),
            Check("expenditure_totals_match_parts", "WARNING", "abs(total_expenditures - (`70pct_ldrrmf_expenditures` + `30pct_quick_response_fund_expenditures`)) > 0.01"),
            Check("duplicate_lgu_year_key", "WARNING", key_columns=("_source_ref", "region", "province", "lgu_name", "lgu_type")),
        ),
        ("_source_ref",),
    ),
    Dataset(
        "ahon.bronze.psa_population_raw",
        "psa_population",
        ("geographic_location", "total_population", "urban_population", "percent_urban", "census_year"),
        (
            Check("required_population_fields", "WARNING", "geographic_location IS NULL OR trim(geographic_location) = '' OR census_year IS NULL"),
            Check("numeric_population_values", "WARNING", "try_cast(total_population AS DECIMAL(20, 2)) IS NULL OR try_cast(urban_population AS DECIMAL(20, 2)) IS NULL OR try_cast(percent_urban AS DECIMAL(8, 4)) IS NULL"),
            Check("population_ranges", "WARNING", "try_cast(total_population AS DECIMAL(20, 2)) < 0 OR try_cast(urban_population AS DECIMAL(20, 2)) < 0 OR try_cast(percent_urban AS DECIMAL(8, 4)) < 0 OR try_cast(percent_urban AS DECIMAL(8, 4)) > 100"),
            Check("urban_population_not_over_total", "WARNING", "try_cast(urban_population AS DECIMAL(20, 2)) > try_cast(total_population AS DECIMAL(20, 2))"),
            Check("duplicate_path_year_key", "WARNING", key_columns=("geographic_location", "census_year")),
        ),
        ("census_year",),
    ),
    Dataset(
        "ahon.bronze.philvolcs_earthquake_data",
        "philvolcs_earthquake_data",
        ("Date-Time", "Latitude", "Longitude", "Depth", "Magnitude", "Location", "Month", "Year"),
        (
            Check("event_timestamp_parseable", "WARNING", "try_to_timestamp(`Date-Time`, 'dd MMMM yyyy - hh:mm a') IS NULL"),
            Check("coordinates_in_global_range", "WARNING", "Latitude IS NULL OR Latitude < -90 OR Latitude > 90 OR Longitude IS NULL OR Longitude < -180 OR Longitude > 180"),
            Check("nonnegative_depth_and_magnitude", "WARNING", "Depth IS NULL OR Depth < 0 OR Magnitude IS NULL OR Magnitude < 0"),
            Check("source_month_year_match_timestamp", "WARNING", "try_to_timestamp(`Date-Time`, 'dd MMMM yyyy - hh:mm a') IS NOT NULL AND (try_cast(Year AS INT) <> year(try_to_timestamp(`Date-Time`, 'dd MMMM yyyy - hh:mm a')) OR lower(trim(Month)) <> lower(date_format(try_to_timestamp(`Date-Time`, 'dd MMMM yyyy - hh:mm a'), 'MMMM')) )"),
            Check("duplicate_source_row_hash", "WARNING", key_columns=("_row_hash",)),
        ),
        ("Year", "Month"),
    ),
    Dataset(
        "ahon.bronze.psgc",
        "psgc",
        ("psgc_code", "area_name", "geographic_level", "version"),
        (
            Check("psgc_code_format", "WARNING", "psgc_code IS NULL OR psgc_code NOT RLIKE '^[0-9]{10}$'"),
            Check("required_geography", "WARNING", "area_name IS NULL OR trim(area_name) = '' OR geographic_level IS NULL OR trim(geographic_level) = '' OR version IS NULL OR trim(version) = ''"),
            Check("duplicate_code_version", "WARNING", key_columns=("psgc_code", "version")),
        ),
        ("version",),
    ),
    Dataset(
        "ahon.bronze.cmci_raw_indicator_batch_html",
        "cmci_data_portal",
        ("batch_id", "requested_psgc_codes", "requested_cmci_names", "requested_years", "requested_indicator_codes", "expected_lgu_count", "expected_year_count", "expected_indicator_count", "returned_value_count", "response_html", "response_hash", "ingestion_timestamp"),
        (
            Check("request_arrays_match_expected_counts", "WARNING", "size(requested_psgc_codes) <> expected_lgu_count OR size(requested_cmci_names) <> expected_lgu_count OR size(requested_years) <> expected_year_count OR size(requested_indicator_codes) <> expected_indicator_count"),
            Check("returned_value_count_matches_request", "WARNING", "returned_value_count <> expected_lgu_count * expected_year_count * expected_indicator_count"),
            Check("response_hash_format", "WARNING", "response_hash IS NULL OR response_hash NOT RLIKE '^[0-9a-fA-F]{64}$'"),
            Check("unique_batch_id", "WARNING", key_columns=("batch_id",)),
        ),
        (),
    ),
    Dataset(
        "ahon.bronze.cmci_raw_indicator",
        "cmci_data_portal",
        ("batch_id", "psgc_code", "psgc_name", "cmci_name", "indicator_label", "year", "raw_value", "response_hash", "ingestion_timestamp"),
        (
            Check("required_indicator_identity", "WARNING", "batch_id IS NULL OR psgc_code IS NULL OR cmci_name IS NULL OR indicator_label IS NULL OR year IS NULL"),
            Check("reporting_year_numeric", "WARNING", "try_cast(year AS INT) IS NULL"),
            Check("indicator_value_numeric_or_missing", "WARNING", "raw_value IS NOT NULL AND trim(raw_value) NOT IN ('', '-') AND try_cast(raw_value AS DOUBLE) IS NULL"),
            Check("duplicate_batch_lgu_indicator_year", "WARNING", key_columns=("batch_id", "psgc_code", "indicator_label", "year")),
        ),
        ("year",),
    ),
    Dataset(
        "ahon.bronze.geoportal_city_municipality_boundary",
        "geoportal_city_municipality_boundary",
        ("feature_json",),
        (
            Check("feature_json_present", "BLOCKING", "feature_json IS NULL OR trim(feature_json) = ''"),
            Check("feature_geometry_present", "WARNING", "get_json_object(feature_json, '$.geometry') IS NULL"),
            Check("duplicate_source_feature", "WARNING", key_columns=("_source_ref", "_row_hash")),
        ),
        ("_source_ref",),
    ),
)

CATALOG = os.environ.get("AHON_CATALOG", "ahon")
DATASETS = tuple(
    replace(dataset, table_name=dataset.table_name.replace("ahon.", f"{CATALOG}.", 1))
    for dataset in DATASETS
)


def result_status(failed_count: int, severity: str) -> str:
    """Return the stable outcome label used by persisted quality results."""
    if failed_count == 0:
        return "PASS"
    return "FAIL" if severity == "BLOCKING" else "WARN"


def schema_drift(
    expected_columns: set[str], actual_columns: set[str]
) -> tuple[list[str], list[str]]:
    """Separate missing required columns from extra source columns."""
    return (
        sorted(expected_columns - actual_columns),
        sorted(actual_columns - expected_columns),
    )


def main() -> None:
    """Aggregate each table once and append per-rule outcomes to monitoring."""
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException
    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    run_id = str(uuid.uuid4())
    checked_at = datetime.now(timezone.utc)
    results = []
    blocking_failures = []

    for dataset in DATASETS:
        try:
            frame = spark.table(dataset.table_name)
        except AnalysisException as error:
            blocking_failures.append(dataset.table_name)
            results.append(
                (
                    run_id,
                    checked_at,
                    dataset.source_name,
                    dataset.table_name,
                    "table_available",
                    "BLOCKING",
                    "FAIL",
                    0,
                    1,
                    str(error)[:1000],
                )
            )
            continue

        expected_columns = set(dataset.source_columns) | set(PROVENANCE_COLUMNS)
        missing_columns, extra_columns = schema_drift(
            expected_columns, set(frame.columns)
        )
        if missing_columns:
            blocking_failures.append(dataset.table_name)
            results.append(
                (
                    run_id,
                    checked_at,
                    dataset.source_name,
                    dataset.table_name,
                    "required_schema",
                    "BLOCKING",
                    "FAIL",
                    int(frame.count()),
                    1,
                    json.dumps({"missing_columns": missing_columns}),
                )
            )
            continue

        checks = list(dataset.checks)
        for column in PROVENANCE_COLUMNS:
            checks.append(
                Check(
                    f"provenance_{column.lstrip('_')}_present",
                    "BLOCKING",
                    f"{column} IS NULL OR CAST({column} AS STRING) = ''",
                )
            )
        checks.append(
            Check(
                "row_hash_sha256_format",
                "BLOCKING",
                "_row_hash IS NULL OR _row_hash NOT RLIKE '^[0-9a-fA-F]{64}$'",
            )
        )

        expressions = [F.count(F.lit(1)).alias("__row_count")]
        for index, check in enumerate(checks):
            if check.key_columns:
                distinct_key = F.countDistinct(
                    F.struct(*[F.col(name) for name in check.key_columns])
                )
                expressions.append(
                    (F.count(F.lit(1)) - distinct_key).alias(f"__check_{index}")
                )
            else:
                expressions.append(
                    F.sum(F.when(F.expr(check.predicate), 1).otherwise(0)).alias(
                        f"__check_{index}"
                    )
                )
        expressions.extend(
            (
                F.min("_ingested_at").cast("string").alias("__min_ingested_at"),
                F.max("_ingested_at").cast("string").alias("__max_ingested_at"),
                F.countDistinct("_batch_id").alias("__batch_count"),
                F.max_by(F.col("_batch_id"), F.col("_ingested_at")).alias(
                    "__latest_batch_id"
                ),
                F.sort_array(F.collect_set("_source_ref")).alias("__source_refs"),
            )
        )
        for index, column in enumerate(dataset.coverage_columns):
            expressions.append(
                F.sort_array(F.collect_set(F.col(column).cast("string"))).alias(
                    f"__coverage_{index}"
                )
            )
        summary = frame.agg(*expressions).first()
        row_count = int(summary["__row_count"] or 0)

        if extra_columns:
            results.append(
                (
                    run_id,
                    checked_at,
                    dataset.source_name,
                    dataset.table_name,
                    "unexpected_source_columns",
                    "WARNING",
                    "WARN",
                    row_count,
                    len(extra_columns),
                    json.dumps({"unexpected_columns": extra_columns}),
                )
            )

        freshness = {
            "min_ingested_at": summary["__min_ingested_at"],
            "max_ingested_at": summary["__max_ingested_at"],
            "batch_count": int(summary["__batch_count"] or 0),
            "latest_batch_id": summary["__latest_batch_id"],
            "source_refs": summary["__source_refs"] or [],
            "coverage": {
                column: summary[f"__coverage_{index}"] or []
                for index, column in enumerate(dataset.coverage_columns)
            },
        }
        if row_count == 0:
            blocking_failures.append(dataset.table_name)
            results.append(
                (
                    run_id,
                    checked_at,
                    dataset.source_name,
                    dataset.table_name,
                    "table_not_empty",
                    "BLOCKING",
                    "FAIL",
                    row_count,
                    1,
                    json.dumps(freshness, sort_keys=True),
                )
            )
        for index, check in enumerate(checks):
            failed_count = int(summary[f"__check_{index}"] or 0)
            status = result_status(failed_count, check.severity)
            if status == "FAIL":
                blocking_failures.append(f"{dataset.table_name}:{check.name}")
            results.append(
                (
                    run_id,
                    checked_at,
                    dataset.source_name,
                    dataset.table_name,
                    check.name,
                    check.severity,
                    status,
                    row_count,
                    failed_count,
                    json.dumps(freshness, sort_keys=True),
                )
            )

    schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("checked_at", TimestampType(), False),
            StructField("dataset_name", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("rule_name", StringType(), False),
            StructField("severity", StringType(), False),
            StructField("status", StringType(), False),
            StructField("row_count", LongType(), False),
            StructField("failed_count", LongType(), False),
            StructField("details", StringType(), True),
        ]
    )
    spark.createDataFrame(results, schema).write.mode("append").saveAsTable(
        f"{CATALOG}.monitoring.dq_result"
    )

    print(f"Quality run {run_id}: {len(results)} rules across {len(DATASETS)} tables")
    if blocking_failures:
        raise RuntimeError(
            "Blocking Bronze quality failures: " + ", ".join(blocking_failures)
        )


if __name__ == "__main__":
    main()
