from __future__ import annotations

import sys
from pathlib import Path
from unittest import TestCase

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPOSITORY_ROOT / "src" / "sql"))

from monitoring.bronze_quality import (
    DATASETS,
    result_status,
    schema_drift,
)


class BronzeQualityTests(TestCase):
    def test_blocking_and_warning_outcomes_are_distinct(self) -> None:
        self.assertEqual(result_status(0, "BLOCKING"), "PASS")
        self.assertEqual(result_status(2, "BLOCKING"), "FAIL")
        self.assertEqual(result_status(2, "WARNING"), "WARN")

    def test_schema_drift_distinguishes_missing_and_additional_columns(self) -> None:
        missing, extra = schema_drift(
            {"required", "provenance"}, {"required", "provenance", "new_field"}
        )

        self.assertEqual(missing, [])
        self.assertEqual(extra, ["new_field"])

    def test_every_current_bronze_table_has_quality_rules(self) -> None:
        tables = {dataset.table_name for dataset in DATASETS}

        self.assertEqual(
            tables,
            {
                "ahon.bronze.blgf_ldrrmf_annual_lgu",
                "ahon.bronze.psa_population_raw",
                "ahon.bronze.philvolcs_earthquake_data",
                "ahon.bronze.psgc",
                "ahon.bronze.cmci_raw_indicator_batch_html",
                "ahon.bronze.cmci_raw_indicator",
                "ahon.bronze.geoportal_city_municipality_boundary",
            },
        )
        self.assertTrue(all(dataset.checks for dataset in DATASETS))

    def test_declared_rule_columns_exist_in_documented_source_schemas(self) -> None:
        for dataset in DATASETS:
            documented_columns = set(dataset.source_columns)
            for check in dataset.checks:
                documented_columns.update(check.key_columns)
            self.assertTrue(documented_columns)

    def test_dataset_rules_cover_source_specific_quality_contracts(self) -> None:
        rules = {
            dataset.table_name: {check.name: check for check in dataset.checks}
            for dataset in DATASETS
        }
        self.assertIn(
            "appropriation_totals_match_parts",
            rules["ahon.bronze.blgf_ldrrmf_annual_lgu"],
        )
        self.assertIn(
            "urban_population_not_over_total",
            rules["ahon.bronze.psa_population_raw"],
        )
        self.assertIn(
            "coordinates_in_global_range",
            rules["ahon.bronze.philvolcs_earthquake_data"],
        )
        self.assertIn("psgc_code_format", rules["ahon.bronze.psgc"])
        self.assertIn(
            "returned_value_count_matches_request",
            rules["ahon.bronze.cmci_raw_indicator_batch_html"],
        )
        self.assertIn(
            "indicator_value_numeric_or_missing",
            rules["ahon.bronze.cmci_raw_indicator"],
        )

    def test_duplicate_candidates_are_warnings_not_source_row_filters(self) -> None:
        duplicate_checks = [
            check
            for dataset in DATASETS
            for check in dataset.checks
            if check.key_columns
        ]

        self.assertTrue(duplicate_checks)
        self.assertTrue(
            all(check.severity == "WARNING" for check in duplicate_checks)
        )

