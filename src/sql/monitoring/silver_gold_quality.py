"""Validate Silver and Gold contracts and append outcomes to dq_result."""

from __future__ import annotations

import json
import os
import sys
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from bronze_quality import result_status, schema_drift

CATALOG = os.environ.get("AHON_CATALOG", "ahon")


def table_name(name: str) -> str:
    return name.replace("ahon.", f"{CATALOG}.", 1)


BLGF_SILVER = table_name("ahon.silver.blgf_ldrrmf_annual_lgu_clean")

CMCI_CODE_DIR = Path(__file__).resolve().parents[1] / "01_bronze_ingest"
sys.path.insert(0, str(CMCI_CODE_DIR))
from cmci_common import INDICATORS_BY_PILLAR  # noqa: E402

Severity = Literal["BLOCKING", "WARNING"]


@dataclass(frozen=True)
class Rule:
    name: str
    severity: Severity
    predicate: str | None = None
    key_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class Contract:
    layer: str
    dataset: str
    table: str
    columns: tuple[str, ...]
    rules: tuple[Rule, ...] = ()
    coverage_columns: tuple[str, ...] = ()


PROVENANCE = ("_source_name", "_source_ref", "_ingested_at", "_batch_id", "_row_hash")
CMCI_COMMON = (
    "psgc_code",
    "year",
    "lgu",
    "cmci_name",
    "source_response_hash",
    "source_ingestion_timestamp",
    "silver_processed_timestamp",
)


def contracts() -> tuple[Contract, ...]:
    pillars = []
    for pillar, indicators in INDICATORS_BY_PILLAR.items():
        suffix = pillar.lower().replace(" ", "_")
        pillars.append(
            Contract(
                "silver",
                "cmci",
                f"ahon.silver.cmci_{suffix}",
                (*CMCI_COMMON, *indicators.values()),
                (
                    Rule("required_identity", "BLOCKING", "psgc_code IS NULL OR year IS NULL"),
                    Rule("unique_lgu_year", "BLOCKING", key_columns=("psgc_code", "year")),
                    *[
                        Rule(
                            f"missing_{code}_score",
                            "WARNING",
                            f"{code} IS NULL",
                        )
                        for code in indicators.values()
                    ],
                ),
                ("year",),
            )
        )

    return (
        Contract(
            "silver",
            "psa_population",
            "ahon.silver.psa_population_clean",
            (
                "geographic_location", "geographic_level", "region_name", "province_name",
                "lgu_name", "census_year", "total_population", "urban_population",
                "percent_urban", *PROVENANCE,
            ),
            (
                Rule("required_geographic_key", "BLOCKING", "geographic_location IS NULL OR census_year IS NULL"),
                Rule("unique_path_year", "BLOCKING", key_columns=("geographic_location", "census_year")),
                Rule("population_values_nonnegative", "WARNING", "total_population < 0 OR urban_population < 0 OR percent_urban < 0 OR percent_urban > 100"),
                Rule("typed_values_present", "WARNING", "total_population IS NULL OR urban_population IS NULL OR percent_urban IS NULL"),
            ),
            ("census_year", "geographic_level"),
        ),
        Contract(
            "silver",
            "blgf_ldrrmf_annual_lgu",
            "ahon.silver.blgf_ldrrmf_annual_lgu_clean",
            (
                "fiscal_year", "region", "province", "lgu_name", "lgu_type",
                "70pct_ldrrmf_budget_appropriation", "70pct_ldrrmf_expenditures",
                "30pct_quick_response_fund_budget_appropriation",
                "30pct_quick_response_fund_expenditures", "total_budget_appropriation",
                "total_expenditures", *PROVENANCE,
            ),
            (
                Rule("fiscal_year_from_source_ref", "WARNING", "fiscal_year IS NULL OR fiscal_year < 1900"),
                Rule("required_lgu_name", "WARNING", "lgu_name IS NULL OR trim(lgu_name) = ''"),
                Rule("supported_lgu_type", "WARNING", "lgu_type IS NULL OR lgu_type NOT IN ('Province', 'City', 'Municipality')"),
                Rule("typed_amounts_present", "WARNING", "total_budget_appropriation IS NULL OR total_expenditures IS NULL"),
                Rule("appropriation_parts_match_total", "WARNING", "abs(total_budget_appropriation - (`70pct_ldrrmf_budget_appropriation` + `30pct_quick_response_fund_budget_appropriation`)) > 0.01"),
                Rule("expenditure_parts_match_total", "WARNING", "abs(total_expenditures - (`70pct_ldrrmf_expenditures` + `30pct_quick_response_fund_expenditures`)) > 0.01"),
            ),
            ("fiscal_year",),
        ),
        Contract(
            "silver",
            "philvolcs_earthquake",
            "ahon.silver.philvolcs_earthquake_data_clean",
            (
                "id", "event_time", "latitude", "longitude", "depth", "magnitude",
                "location_description", "month", "year", *PROVENANCE,
            ),
            (
                Rule("required_event_key", "BLOCKING", "id IS NULL OR event_time IS NULL"),
                Rule("duplicate_event_key", "WARNING", key_columns=("id",)),
                Rule("coordinates_valid", "BLOCKING", "latitude IS NULL OR latitude < -90 OR latitude > 90 OR longitude IS NULL OR longitude < -180 OR longitude > 180"),
                Rule("measures_nonnegative", "BLOCKING", "depth IS NULL OR depth < 0 OR magnitude IS NULL OR magnitude < 0"),
            ),
            ("year", "month"),
        ),
        Contract(
            "silver",
            "psgc",
            "ahon.silver.psgc_clean",
            (
                "psgc_code", "area_name", "geographic_level", "region_code", "province_code",
                "municipality_code", "barangay_code", "correspondence_code", "old_name",
                "city_class", "income_classification", "urban_rural", "island_region", "status",
                "version", "populations_json", *PROVENANCE,
            ),
            (
                Rule("required_geographic_identity", "BLOCKING", "psgc_code IS NULL OR area_name IS NULL OR geographic_level IS NULL OR version IS NULL"),
                Rule("unique_code_version", "BLOCKING", key_columns=("psgc_code", "version")),
                Rule("psgc_code_format", "WARNING", "psgc_code NOT RLIKE '^[0-9]{10}$'"),
            ),
            ("version", "geographic_level"),
        ),
        Contract(
            "silver",
            "cmci",
            "ahon.silver.cmci_ingestion_batch_clean",
            (
                "batch_id", "requested_psgc_codes", "requested_cmci_names", "requested_years",
                "requested_indicator_codes", "expected_lgu_count", "expected_year_count",
                "expected_indicator_count", "returned_value_count", "response_hash",
                "ingestion_timestamp", "is_complete", *PROVENANCE, "silver_processed_timestamp",
            ),
            (
                Rule("unique_batch_id", "BLOCKING", key_columns=("batch_id",)),
                Rule("incomplete_batch_review", "WARNING", "NOT is_complete"),
            ),
        ),
        *pillars,
        Contract(
            "gold",
            "geography",
            "ahon.gold.dim_lgu",
            ("psgc_code", "area_name", "geographic_level", "region_code", "province_code", "municipality_code", "barangay_code"),
            (
                Rule("required_lgu_dimension", "BLOCKING", "psgc_code IS NULL OR area_name IS NULL OR geographic_level IS NULL"),
                Rule("unique_psgc_code", "BLOCKING", key_columns=("psgc_code",)),
                Rule("missing_hierarchy_codes", "WARNING", "region_code IS NULL OR municipality_code IS NULL"),
            ),
        ),
        Contract(
            "gold",
            "cmci",
            "ahon.gold.dim_cmci_indicator",
            ("indicator_code", "pillar_name", "indicator_name"),
            (
                Rule("required_indicator_fields", "BLOCKING", "indicator_code IS NULL OR pillar_name IS NULL OR indicator_name IS NULL"),
                Rule("unique_indicator_code", "BLOCKING", key_columns=("indicator_code",)),
            ),
        ),
        Contract(
            "gold",
            "cmci",
            "ahon.gold.fact_cmci_indicator",
            ("psgc_code", "year", "indicator_code", "psgc_name", "cmci_name", "geographic_level", "raw_score"),
            (
                Rule("required_fact_key", "BLOCKING", "psgc_code IS NULL OR year IS NULL OR indicator_code IS NULL"),
                Rule("unique_lgu_year_indicator", "BLOCKING", key_columns=("psgc_code", "year", "indicator_code")),
                Rule("missing_indicator_score", "WARNING", "raw_score IS NULL"),
            ),
        ),
        Contract(
            "gold",
            "psa_population",
            "ahon.gold.fact_population",
            (
                "geographic_location", "psgc_code", "year", "total_population",
                "urban_population", "percent_urban",
            ),
            (
                Rule("required_population_grain", "BLOCKING", "geographic_location IS NULL OR year IS NULL"),
                Rule("unique_city_municipality_year", "BLOCKING", key_columns=("geographic_location", "year")),
                Rule("unmatched_or_ambiguous_lgu", "WARNING", "psgc_code IS NULL"),
                Rule("population_values_valid", "WARNING", "total_population IS NULL OR urban_population IS NULL OR total_population < 0 OR urban_population < 0 OR percent_urban IS NULL OR percent_urban < 0 OR percent_urban > 100"),
            ),
            ("year",),
        ),
        Contract(
            "gold",
            "blgf_ldrrmf_annual_lgu",
            "ahon.gold.fact_ldrrmf",
            (
                "ldrrmf_fact_key", "psgc_code", "fiscal_year", "region", "province", "lgu_name",
                "lgu_type", "appropriation_70_pct", "expenditure_70_pct", "appropriation_30_pct",
                "expenditure_30_pct", "total_appropriation", "total_expenditure", "utilization_rate",
                "match_status", "match_confidence",
            ),
            (
                Rule("unique_fact_key", "BLOCKING", key_columns=("ldrrmf_fact_key",)),
                Rule("city_municipality_scope", "BLOCKING", "lgu_type IS NULL OR lgu_type NOT IN ('City', 'Municipality')"),
                Rule("unmatched_lgu_rows", "WARNING", "match_status = 'UNMATCHED' OR psgc_code IS NULL"),
                Rule("utilization_rate_missing_for_calculable_row", "BLOCKING", "total_appropriation <> 0 AND total_appropriation IS NOT NULL AND total_expenditure IS NOT NULL AND utilization_rate IS NULL"),
                Rule("utilization_rate_formula", "BLOCKING", "total_appropriation <> 0 AND total_appropriation IS NOT NULL AND total_expenditure IS NOT NULL AND abs(utilization_rate - total_expenditure * 100 / total_appropriation) > 0.01"),
            ),
            ("fiscal_year",),
        ),
        Contract(
            "gold",
            "philvolcs_earthquake",
            "ahon.gold.fact_earthquake_event",
            (
                "earthquake_fact_key", "psgc_code", "location", "timestamp", "depth", "magnitude",
                "longitude", "latitude", "match_status", "match_confidence",
            ),
            (
                Rule("unique_event_key", "BLOCKING", key_columns=("earthquake_fact_key",)),
                Rule("unmatched_event_rows", "WARNING", "match_status = 'UNMATCHED' OR psgc_code IS NULL"),
            ),
            ("timestamp",),
        ),
    )


def main() -> None:
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.utils import AnalysisException
    from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    run_id = str(uuid.uuid4())
    checked_at = datetime.now(timezone.utc)
    results = []
    blocking = []
    row_counts = {}

    for base_contract in contracts():
        contract = replace(base_contract, table=table_name(base_contract.table))
        try:
            frame = spark.table(contract.table)
        except AnalysisException as error:
            blocking.append(contract.table)
            results.append((run_id, checked_at, contract.dataset, contract.table, "table_available", "BLOCKING", "FAIL", 0, 1, json.dumps({"layer": contract.layer, "error": str(error)[:500]})))
            continue

        missing, extra = schema_drift(set(contract.columns), set(frame.columns))
        if missing:
            blocking.append(contract.table)
            results.append((run_id, checked_at, contract.dataset, contract.table, "required_schema", "BLOCKING", "FAIL", 0, 1, json.dumps({"layer": contract.layer, "missing_columns": missing})))
            continue

        checks = list(contract.rules)
        for column in PROVENANCE:
            if column in frame.columns:
                checks.append(
                    Rule(
                        f"provenance_{column.lstrip('_')}_present",
                        "BLOCKING",
                        f"{column} IS NULL OR CAST({column} AS STRING) = ''",
                    )
                )
        if "_row_hash" in frame.columns:
            checks.append(
                Rule(
                    "row_hash_sha256_format",
                    "BLOCKING",
                    "_row_hash NOT RLIKE '^[0-9a-fA-F]{64}$'",
                )
            )
        if "source_response_hash" in frame.columns:
            checks.extend(
                (
                    Rule(
                        "response_hash_sha256_format",
                        "BLOCKING",
                        "source_response_hash IS NULL OR source_response_hash NOT RLIKE '^[0-9a-fA-F]{64}$'",
                    ),
                    Rule(
                        "source_ingestion_timestamp_present",
                        "BLOCKING",
                        "source_ingestion_timestamp IS NULL",
                    ),
                )
            )
        expressions = [F.count(F.lit(1)).alias("row_count")]
        for index, rule in enumerate(checks):
            if rule.key_columns:
                distinct_count = F.countDistinct(F.struct(*rule.key_columns))
                expressions.append((F.count(F.lit(1)) - distinct_count).alias(f"check_{index}"))
            else:
                expressions.append(F.sum(F.when(F.expr(rule.predicate), 1).otherwise(0)).alias(f"check_{index}"))
        timestamp_columns = [
            column
            for column in ("_ingested_at", "source_ingestion_timestamp", "silver_processed_timestamp", "timestamp")
            if column in frame.columns
        ]
        for index, column in enumerate(timestamp_columns):
            expressions.extend((
                F.min(F.col(column)).cast("string").alias(f"min_ts_{index}"),
                F.max(F.col(column)).cast("string").alias(f"max_ts_{index}"),
            ))
        for index, column in enumerate(contract.coverage_columns):
            expressions.append(F.countDistinct(F.col(column)).alias(f"coverage_{index}"))

        summary = frame.agg(*expressions).first()
        row_count = int(summary["row_count"] or 0)
        row_counts[contract.table] = row_count
        details = {
            "layer": contract.layer,
            "coverage": {
                column: int(summary[f"coverage_{index}"] or 0)
                for index, column in enumerate(contract.coverage_columns)
            },
            "time_bounds": {
                column: {
                    "min": summary[f"min_ts_{index}"],
                    "max": summary[f"max_ts_{index}"],
                }
                for index, column in enumerate(timestamp_columns)
            },
        }

        if extra:
            results.append((run_id, checked_at, contract.dataset, contract.table, "unexpected_source_columns", "WARNING", "WARN", row_count, len(extra), json.dumps({**details, "unexpected_columns": extra}, sort_keys=True)))
        if row_count == 0:
            blocking.append(contract.table)
            results.append((run_id, checked_at, contract.dataset, contract.table, "table_not_empty", "BLOCKING", "FAIL", row_count, 1, json.dumps(details, sort_keys=True)))
        for index, rule in enumerate(checks):
            failed_count = int(summary[f"check_{index}"] or 0)
            status = result_status(failed_count, rule.severity)
            if status == "FAIL":
                blocking.append(f"{contract.table}:{rule.name}")
            results.append((run_id, checked_at, contract.dataset, contract.table, rule.name, rule.severity, status, row_count, failed_count, json.dumps(details, sort_keys=True)))

    def record_reconciliation(
        layer: str,
        dataset: str,
        table: str,
        rule: str,
        expected: int,
        actual: int,
    ) -> None:
        difference = abs(expected - actual)
        status = result_status(difference, "BLOCKING")
        if status == "FAIL":
            blocking.append(f"{table}:{rule}")
        results.append(
            (
                run_id,
                checked_at,
                dataset,
                table,
                rule,
                "BLOCKING",
                status,
                actual,
                difference,
                json.dumps(
                    {"layer": layer, "expected_count": expected, "actual_count": actual},
                    sort_keys=True,
                ),
            )
        )

    def record_unique_grain(
        dataset: str, table: str, key_columns: tuple[str, ...]
    ) -> None:
        duplicate_groups = (
            spark.table(table)
            .groupBy(*key_columns)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        status = result_status(duplicate_groups, "BLOCKING")
        if status == "FAIL":
            blocking.append(f"{table}:{'_'.join(key_columns)}_unique_grain")
        results.append(
            (
                run_id,
                checked_at,
                dataset,
                table,
                "unique_" + "_".join(key_columns),
                "BLOCKING",
                status,
                row_counts.get(table, 0),
                duplicate_groups,
                json.dumps(
                    {"layer": "gold", "grain_columns": key_columns},
                    sort_keys=True,
                ),
            )
        )

    # Exact source/target reconciliations are used only where the Silver job
    # promises a snapshot or a defined key-level merge.
    if table_name("ahon.silver.blgf_ldrrmf_annual_lgu_clean") in row_counts:
        record_reconciliation(
            "silver",
            "blgf_ldrrmf_annual_lgu",
            table_name("ahon.silver.blgf_ldrrmf_annual_lgu_clean"),
            "bronze_silver_row_count",
            spark.table(table_name("ahon.bronze.blgf_ldrrmf_annual_lgu")).count(),
            row_counts[table_name("ahon.silver.blgf_ldrrmf_annual_lgu_clean")],
        )
    if table_name("ahon.silver.psgc_clean") in row_counts:
        record_reconciliation(
            "silver",
            "psgc",
            table_name("ahon.silver.psgc_clean"),
            "bronze_silver_row_count",
            spark.table(table_name("ahon.bronze.psgc")).count(),
            row_counts[table_name("ahon.silver.psgc_clean")],
        )
    if table_name("ahon.silver.cmci_ingestion_batch_clean") in row_counts:
        record_reconciliation(
            "silver",
            "cmci",
            table_name("ahon.silver.cmci_ingestion_batch_clean"),
            "bronze_silver_batch_count",
            spark.table(table_name("ahon.bronze.cmci_raw_indicator_batch_html")).count(),
            row_counts[table_name("ahon.silver.cmci_ingestion_batch_clean")],
        )
    if table_name("ahon.silver.psa_population_clean") in row_counts:
        psa = spark.table(table_name("ahon.bronze.psa_population_raw"))
        expected_psa_keys = (
            psa.select(
                F.regexp_replace("geographic_location", r"\s+1/$", "").alias(
                    "geographic_location"
                ),
                "census_year",
            )
            .distinct()
            .count()
        )
        record_reconciliation(
            "silver",
            "psa_population",
            table_name("ahon.silver.psa_population_clean"),
            "distinct_bronze_key_silver_row_count",
            expected_psa_keys,
            row_counts[table_name("ahon.silver.psa_population_clean")],
        )
    if table_name("ahon.silver.philvolcs_earthquake_data_clean") in row_counts:
        quake = spark.table(table_name("ahon.bronze.philvolcs_earthquake_data"))
        parsed_time = F.expr("try_to_timestamp(`Date-Time`, 'dd MMMM yyyy - hh:mm a')")
        valid_quake_count = quake.filter(
            (F.trim(F.col("Date-Time")) != "")
            & parsed_time.isNotNull()
            & F.col("Latitude").between(-90, 90)
            & F.col("Longitude").between(-180, 180)
            & (F.col("Depth") >= 0)
            & (F.col("Magnitude") >= 0)
        ).count()
        record_reconciliation(
            "silver",
            "philvolcs_earthquake",
            table_name("ahon.silver.philvolcs_earthquake_data_clean"),
            "valid_bronze_silver_row_count",
            valid_quake_count,
            row_counts[table_name("ahon.silver.philvolcs_earthquake_data_clean")],
        )

    complete_cmci_batches = table_name("ahon.silver.cmci_ingestion_batch_clean")
    cmci_pillars = [
        (table_name("ahon.silver.cmci_" + pillar.lower().replace(" ", "_")), len(indicators))
        for pillar, indicators in INDICATORS_BY_PILLAR.items()
    ]
    if complete_cmci_batches in row_counts and all(
        table in row_counts for table, _ in cmci_pillars
    ):
        batch_ids = (
            spark.table(complete_cmci_batches)
            .filter(F.col("is_complete") == F.lit(True))
            .select("batch_id")
            .distinct()
        )
        expected_pairs = (
            spark.table(table_name("ahon.bronze.cmci_raw_indicator"))
            .join(batch_ids, "batch_id", "inner")
            .select("psgc_code", "year")
            .distinct()
            .count()
        )
        for table, _ in cmci_pillars:
            record_reconciliation(
                "silver", "cmci", table, "complete_batch_lgu_year_count",
                expected_pairs, row_counts[table],
            )

    if table_name("ahon.gold.dim_lgu") in row_counts:
        expected = spark.table(table_name("ahon.reference.lgu_master")).filter(
            F.col("is_active") == F.lit(True)
        ).count()
        record_reconciliation(
            "gold", "geography", table_name("ahon.gold.dim_lgu"), "active_reference_row_count", expected,
            row_counts[table_name("ahon.gold.dim_lgu")],
        )
        hierarchy = spark.table(table_name("ahon.silver.psgc_clean")).groupBy("psgc_code").agg(
            F.countDistinct(
                F.struct(
                    "region_code",
                    "province_code",
                    "municipality_code",
                    "barangay_code",
                )
            ).alias("version_count")
        )
        conflicted_codes = (
            hierarchy.filter(F.col("version_count") > 1)
            .join(
                spark.table(table_name("ahon.reference.lgu_master"))
                .filter(F.col("is_active") == F.lit(True))
                .select("psgc_code"),
                "psgc_code",
                "inner",
            )
            .count()
        )
        results.append(
            (
                run_id,
                checked_at,
                "geography",
                table_name("ahon.gold.dim_lgu"),
                "conflicting_psgc_hierarchy_versions",
                "WARNING",
                result_status(conflicted_codes, "WARNING"),
                row_counts[table_name("ahon.gold.dim_lgu")],
                conflicted_codes,
                json.dumps({"layer": "gold", "hierarchy_codes_are_null_for_conflicts": True}),
            )
        )
        province_conflicts = (
            spark.table(table_name("ahon.reference.lgu_master"))
            .filter(
                (F.col("is_active") == F.lit(True))
                & F.col("province_code").isNotNull()
            )
            .alias("reference")
            .join(
                spark.table(table_name("ahon.silver.psgc_clean"))
                .filter(F.col("province_code").isNotNull())
                .select("psgc_code", "province_code")
                .distinct()
                .alias("psgc"),
                "psgc_code",
                "inner",
            )
            .filter(F.col("reference.province_code") != F.col("psgc.province_code"))
            .select("psgc_code")
            .distinct()
            .count()
        )
        results.append(
            (
                run_id,
                checked_at,
                "geography",
                table_name("ahon.gold.dim_lgu"),
                "province_code_disagrees_with_active_reference",
                "WARNING",
                result_status(province_conflicts, "WARNING"),
                row_counts[table_name("ahon.gold.dim_lgu")],
                province_conflicts,
                json.dumps({"layer": "gold", "dim_lgu_uses": "lgu_master.province_code"}),
            )
        )
    if table_name("ahon.gold.dim_cmci_indicator") in row_counts:
        expected = sum(len(values) for values in INDICATORS_BY_PILLAR.values())
        record_reconciliation(
            "gold", "cmci", table_name("ahon.gold.dim_cmci_indicator"), "approved_indicator_count",
            expected, row_counts[table_name("ahon.gold.dim_cmci_indicator")],
        )
    if table_name("ahon.gold.fact_population") in row_counts:
        psa_silver = spark.table(table_name("ahon.silver.psa_population_clean"))
        expected = psa_silver.filter(
            F.col("geographic_level") == "City/Municipality"
        ).count()
        record_reconciliation(
            "gold", "psa_population", table_name("ahon.gold.fact_population"),
            "city_municipality_silver_gold_row_count", expected,
            row_counts[table_name("ahon.gold.fact_population")],
        )
        excluded_levels = {
            row["geographic_level"] or "<NULL>": row["count"]
            for row in psa_silver.filter(
                (F.col("geographic_level") != "City/Municipality")
                | F.col("geographic_level").isNull()
            ).groupBy("geographic_level").count().collect()
        }
        excluded_population_count = sum(excluded_levels.values())
        results.append((run_id, checked_at, "psa_population", table_name("ahon.gold.fact_population"), "non_city_municipality_rows_excluded_by_scope", "WARNING", result_status(excluded_population_count, "WARNING"), row_counts[table_name("ahon.gold.fact_population")], excluded_population_count, json.dumps({"layer": "gold", "excluded_source_geographic_levels": excluded_levels, "rows_remain_in": table_name("ahon.silver.psa_population_clean")}, sort_keys=True)))
        record_unique_grain(
            "psa_population",
            table_name("ahon.gold.fact_population"),
            ("geographic_location", "year"),
        )
    if table_name("ahon.gold.fact_ldrrmf") in row_counts:
        silver_ldrrmf = spark.table(BLGF_SILVER)
        expected = silver_ldrrmf.filter(
            F.col("lgu_type").isin("City", "Municipality")
        ).count()
        excluded_type_counts = {
            row["lgu_type"] or "<NULL>": row["count"]
            for row in silver_ldrrmf.filter(
                ~F.col("lgu_type").isin("City", "Municipality")
                | F.col("lgu_type").isNull()
            ).groupBy("lgu_type").count().collect()
        }
        excluded_rows = sum(excluded_type_counts.values())
        record_reconciliation(
            "gold", "blgf_ldrrmf_annual_lgu", table_name("ahon.gold.fact_ldrrmf"),
            "city_municipality_silver_gold_row_count", expected,
            row_counts[table_name("ahon.gold.fact_ldrrmf")],
        )
        results.append((run_id, checked_at, "blgf_ldrrmf_annual_lgu", table_name("ahon.gold.fact_ldrrmf"), "non_city_municipality_rows_excluded_by_scope", "WARNING", result_status(excluded_rows, "WARNING"), row_counts[table_name("ahon.gold.fact_ldrrmf")], excluded_rows, json.dumps({"layer": "gold", "excluded_lgu_type_counts": excluded_type_counts, "rows_remain_in": table_name("ahon.silver.blgf_ldrrmf_annual_lgu_clean")}, sort_keys=True)))
    for table, source in (
        (table_name("ahon.gold.fact_earthquake_event"), table_name("ahon.silver.philvolcs_earthquake_data_clean")),
    ):
        if table in row_counts and source in row_counts:
            record_reconciliation(
                "gold", table.rsplit(".", 1)[-1], table, "silver_gold_row_count",
                row_counts[source], row_counts[table],
            )
    if table_name("ahon.gold.fact_cmci_indicator") in row_counts:
        expected = sum(
            row_counts.get(
                table_name("ahon.silver.cmci_" + pillar.lower().replace(" ", "_")), 0
            )
            * len(indicators)
            for pillar, indicators in INDICATORS_BY_PILLAR.items()
        )
        record_reconciliation(
            "gold", "cmci", table_name("ahon.gold.fact_cmci_indicator"),
            "silver_indicator_to_gold_fact_row_count", expected,
            row_counts[table_name("ahon.gold.fact_cmci_indicator")],
        )

    # Referential integrity is checked only after all expected Gold tables exist.
    foreign_keys = (
        (table_name("ahon.gold.fact_cmci_indicator"), "psgc_code", table_name("ahon.gold.dim_lgu"), "psgc_code"),
        (table_name("ahon.gold.fact_cmci_indicator"), "indicator_code", table_name("ahon.gold.dim_cmci_indicator"), "indicator_code"),
        (table_name("ahon.gold.fact_population"), "psgc_code", table_name("ahon.gold.dim_lgu"), "psgc_code"),
    )
    for child_table, child_column, parent_table, parent_column in foreign_keys:
        if not spark.catalog.tableExists(child_table) or not spark.catalog.tableExists(parent_table):
            continue
        child_keys = (
            spark.table(child_table)
            .filter(F.col(child_column).isNotNull())
            .select(child_column)
            .distinct()
        )
        parent_keys = spark.table(parent_table).select(
            F.col(parent_column).alias("_parent_key")
        ).distinct()
        fk_summary = child_keys.join(
            parent_keys,
            F.col(child_column) == F.col("_parent_key"),
            "left",
        ).agg(
            F.count(F.lit(1)).alias("key_count"),
            F.sum(F.when(F.col("_parent_key").isNull(), 1).otherwise(0)).alias(
                "missing_count"
            ),
        ).first()
        key_count = int(fk_summary["key_count"] or 0)
        missing_count = int(fk_summary["missing_count"] or 0)
        status = result_status(missing_count, "BLOCKING")
        if status == "FAIL":
            blocking.append(f"{child_table}:{child_column}_foreign_key")
        results.append((run_id, checked_at, "cmci", child_table, f"{child_column}_foreign_key", "BLOCKING", status, key_count, missing_count, json.dumps({"layer": "gold", "parent_table": parent_table, "parent_column": parent_column})))

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
        table_name("ahon.monitoring.dq_result")
    )
    print(f"Layer quality run {run_id}: {len(results)} results")
    if blocking:
        raise RuntimeError("Blocking Silver/Gold quality failures: " + ", ".join(blocking))


if __name__ == "__main__":
    main()
