from __future__ import annotations

from pathlib import Path
from unittest import TestCase

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
PLATINUM = (REPOSITORY_ROOT / "src" / "sql" / "platinum" / "build_platinum.py").read_text(encoding="utf-8")


class PlatinumContractTests(TestCase):
    def test_experimental_v1_keeps_approved_formula_and_null_behavior(self) -> None:
        self.assertIn("F.add_months(F.current_timestamp(), -60)", PLATINUM)
        self.assertIn("F.percent_rank()", PLATINUM)
        self.assertIn("population_exposure_score", PLATINUM)
        self.assertIn("earthquake_activity_score", PLATINUM)
        self.assertIn("preparedness_score", PLATINUM)
        self.assertIn("preparedness_gap", PLATINUM)
        self.assertIn("cmci_preparedness_priority_by_indicator", PLATINUM)
        self.assertIn("F.col(\"cmci_capacity_score\").isNotNull()", PLATINUM)

    def test_boundary_silver_name_matches_dataset_naming_standard(self) -> None:
        expected = "geoportal_city_municipality_boundary_clean"
        for path in (
            "src/sql/platinum/load_lgu_boundaries.py",
            "src/sql/platinum/build_platinum.py",
            "src/sql/gold/build_gold.py",
            "docs/architecture/data-dictionary/geoportal_city_municipality_boundary.md",
        ):
            self.assertIn(expected, (REPOSITORY_ROOT / path).read_text(encoding="utf-8"))

    def test_boundary_retry_refreshes_batch_provenance_before_silver_snapshot(self) -> None:
        loader = (REPOSITORY_ROOT / "src/sql/platinum/load_lgu_boundaries.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("whenMatchedUpdate(", loader)
        self.assertIn('"_batch_id": "source._batch_id"', loader)
        self.assertIn('F.col("_batch_id") == run_id', loader)

    def test_outputs_are_quality_checked_before_publication(self) -> None:
        validation = PLATINUM.index("output_specs = (")
        publication = PLATINUM.index('_write(risk, table("platinum", "risk_level_by_lgu"))')
        self.assertLess(validation, publication)
        self.assertIn('"unique_output_grain"', PLATINUM)
        self.assertIn('"current_run_id"', PLATINUM)
        self.assertIn('"risk_level", ("Low", "Medium", "High")', PLATINUM)
        self.assertIn("validation_failures", PLATINUM)
        self.assertIn("metric_formula_null_and_range_consistency", PLATINUM)

    def test_ldrrmf_report_lines_are_aggregated_before_utilization(self) -> None:
        self.assertIn('F.sum("total_appropriation")', PLATINUM)
        self.assertIn('F.sum("total_expenditure")', PLATINUM)
        self.assertIn('"total_expenditure") / F.col("total_appropriation") * 100', PLATINUM)

