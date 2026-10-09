from __future__ import annotations

from pathlib import Path
from unittest import TestCase

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
QUALITY = (REPOSITORY_ROOT / "src" / "sql" / "monitoring" / "silver_gold_quality.py").read_text(encoding="utf-8")


class SilverGoldGrainContractTests(TestCase):
    def test_population_uses_its_documented_path_year_key(self) -> None:
        self.assertIn('("geographic_location", "year")', QUALITY)
        self.assertIn("record_unique_grain(", QUALITY)
        self.assertIn('"BLOCKING"', QUALITY)
