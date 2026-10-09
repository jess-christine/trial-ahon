"""PSA raw CSV decoding and retry contracts, without requiring local Spark."""
from __future__ import annotations

import ast
import codecs
import csv
import os
import tempfile
from pathlib import Path
from types import CodeType, FunctionType
from unittest import TestCase
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[3]


def load_functions(relative_path: str, names: set[str], context: dict) -> dict:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    definitions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    future = ast.parse("from __future__ import annotations").body
    compiled = compile(ast.Module(body=future + definitions, type_ignores=[]), relative_path, "exec")
    for value in compiled.co_consts:
        if isinstance(value, CodeType):
            context[value.co_name] = FunctionType(value, context)
    return context


class PsaSourceRowsTests(TestCase):
    def setUp(self) -> None:
        self.context = load_functions(
            "src/sql/datasets/psa_population/bronze/load_psa_population.py",
            {"read_source_rows", "merge_into_bronze"}, {"csv": csv, "Path": Path},
        )

    def test_declared_charset_preserves_names_values_and_full_hierarchy(self) -> None:
        content = (
            "Geographic Location,Total Population,Urban Population,Percent Urban\n"
            "PHILIPPINES1/,100.00,99.00,99.00\n"
            "..Region IV-A,100.00,99.00,99.00\n"
            "....Laguna,100.00,99.00,99.00\n"
            "......City of Biñan,100.00,99.00,99.00\n"
        )
        for encoding in ("cp1252", "utf-8-sig"):
            with self.subTest(encoding=encoding), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "source.csv"
                path.write_bytes(content.encode(encoding))
                rows = self.context["read_source_rows"](path, "2024", encoding)
                self.assertEqual(len(rows), 4)
                self.assertEqual(rows[-1], (
                    "PHILIPPINES1/ > Region IV-A > Laguna > City of Biñan",
                    "100.00", "99.00", "99.00", "2024",
                ))
                if encoding == "cp1252":
                    with self.assertRaises(UnicodeDecodeError):
                        self.context["read_source_rows"](path, "2024", "utf-8-sig")

    def test_header_drift_is_actionable_and_does_not_filter_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.csv"
            path.write_text("Geographic Location,Unexpected Measure\nLaguna,10\n")
            with self.assertRaisesRegex(ValueError, "PSA CSV schema changed"):
                self.context["read_source_rows"](path, "2024", "cp1252")

    def test_unchanged_rows_keep_batch_provenance_on_retry(self) -> None:
        delta = Mock()
        self.context.update({
            "DeltaTable": delta, "TABLE_NAME": "ahon_test.bronze.psa_population_raw",
            "CREATE_TABLE_SQL": "CREATE TABLE", "MERGE_KEYS": ["geographic_location", "census_year"],
        })
        self.context["merge_into_bronze"](Mock(), Mock())
        merger = delta.forName.return_value.alias.return_value.merge.return_value
        merger.whenMatchedUpdateAll.assert_called_once_with(
            condition="NOT (target._row_hash <=> source._row_hash)"
        )

    def test_extractor_rejects_charset_drift_before_saving(self) -> None:
        requests = Mock()
        response = requests.post.return_value
        response.encoding = "Windows-1252"
        response.headers = {"Content-Type": "text/csv; charset=Windows-1252"}
        context = load_functions(
            "src/sql/datasets/psa_population/extract/extract_psa_population.py", {"fetch_csv"},
            {"requests": requests, "codecs": codecs, "os": os,
             "API_URL": "https://example.invalid", "QUERY_BODY": {}, "REQUEST_TIMEOUT_SECONDS": 60},
        )
        with patch.dict(os.environ, {"AHON_PSA_CSV_ENCODING": "cp1252"}):
            self.assertIs(context["fetch_csv"](), response)
            response.encoding = "utf-8"
            with self.assertRaisesRegex(RuntimeError, "PSA CSV charset changed"):
                context["fetch_csv"]()
