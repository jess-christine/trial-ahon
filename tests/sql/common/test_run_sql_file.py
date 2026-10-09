from __future__ import annotations

import sys
from pathlib import Path
from unittest import TestCase

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPOSITORY_ROOT / "src" / "sql" / "common"))

from run_sql_file import split_statements  # noqa: E402


class SqlFileRunnerTests(TestCase):
    def test_splits_only_unquoted_semicolons_and_keeps_comments(self) -> None:
        statements = split_statements(
            "-- first; comment\nSELECT ';' AS value; /* second; comment */ SELECT 2;"
        )

        self.assertEqual(len(statements), 2)
        self.assertIn("SELECT ';'", statements[0])
        self.assertIn("/* second; comment */", statements[1])

    def test_rejects_unclosed_sql_literals_and_block_comments(self) -> None:
        for sql in ("SELECT 'unfinished", "SELECT 1 /* unfinished"):
            with self.subTest(sql=sql), self.assertRaises(ValueError):
                split_statements(sql)
