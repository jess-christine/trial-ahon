from __future__ import annotations

import importlib.util
import os
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
EXTRACTOR_PATH = (
    REPOSITORY_ROOT
    / "src/sql/datasets/philvolcs_earthquake/extract/philvolcs_earthquake_ingest.py"
)


def load_extractor():
    os.environ.setdefault("AHON_PHIVOLCS_BASE_URL", "https://example.invalid")
    spec = importlib.util.spec_from_file_location(
        "phivolcs_extractor_for_test", EXTRACTOR_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load the PHIVOLCS extractor")
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "pandas": types.ModuleType("pandas"),
            "requests": types.ModuleType("requests"),
        },
    ):
        spec.loader.exec_module(module)
    return module


class PhivolcsMagnitudeRankingTests(TestCase):
    def test_numeric_text_ranks_and_malformed_values_are_reported(self) -> None:
        rank_magnitudes = load_extractor().rank_magnitudes

        ranked, invalid_count = rank_magnitudes(
            {0: "4.2", 1: "not reported", 2: "6.1", 3: None, 4: "NaN"}
        )

        self.assertEqual(ranked, [(6.1, 2), (4.2, 0)])
        self.assertEqual(invalid_count, 3)

    def test_current_month_fallback_does_not_claim_an_earlier_month(self) -> None:
        month_is_current = load_extractor().month_is_current
        current_time = datetime(2026, 10, 9, tzinfo=timezone.utc)

        self.assertFalse(month_is_current(2026, "September", current_time))
        self.assertTrue(month_is_current(2026, "October", current_time))
        self.assertFalse(month_is_current(2025, "October", current_time))
