"""Exact boundary crosswalk behavior; no spatial runtime required."""
import importlib.util
from pathlib import Path
from unittest import TestCase

spec = importlib.util.spec_from_file_location("prepare_boundaries", Path(__file__).resolve().parents[3] / "src/sql/06_reference/prepare_lgu_boundaries.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class BoundaryMappingTests(TestCase):
    def setUp(self):
        self.references = [{"psgc_code": "0102801000", "correspondence_code": "012801000", "lgu_name": "Adams"}]

    def test_code_requires_exact_name_corroboration(self):
        match, status, method = module.map_code("PH0102801", "Adams", self.references)
        self.assertEqual((match["psgc_code"], status, method), ("0102801000", "matched", "code_and_exact_name"))
        self.assertIsNone(module.map_code("PH0102801", "Different", self.references)[0])

    def test_unique_exact_name_can_resolve_changed_code(self):
        self.assertEqual(module.map_code("PH9902801", "Adams", self.references)[1:], ("matched", "unique_exact_name"))

    def test_duplicate_names_and_missing_names_remain_unmatched(self):
        references = self.references + [{"psgc_code": "0202801000", "correspondence_code": "022801000", "lgu_name": "Adams"}]
        self.assertEqual(module.map_code("unknown", "Adams", references)[1], "ambiguous")
        self.assertEqual(module.map_code("unknown", "Adams (Capital)", references)[1], "unmatched")
