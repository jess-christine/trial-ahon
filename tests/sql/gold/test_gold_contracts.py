from __future__ import annotations

import re
import os
import sys
from pathlib import Path
from unittest import TestCase

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SQL_ROOT = REPOSITORY_ROOT / "src" / "sql"
sys.path.insert(0, str(SQL_ROOT))
sys.path.insert(0, str(SQL_ROOT / "monitoring"))

os.environ.setdefault("AHON_CMCI_PORTAL_URL", "https://test.invalid/cmci-portal")
os.environ.setdefault("AHON_CMCI_PROCESS_URL", "https://test.invalid/cmci-process")
os.environ.setdefault("AHON_CMCI_YEARS", "2014,2024")

from gold.build_gold import indicator_records  # noqa: E402
from monitoring.silver_gold_quality import contracts  # noqa: E402


class GoldContractTests(TestCase):
    def test_indicator_dimension_comes_from_the_approved_config(self) -> None:
        records = indicator_records()

        self.assertEqual(len(records), 35)
        self.assertEqual(len({record[0] for record in records}), 35)
        self.assertEqual(records, sorted(records))

    def test_gold_tables_have_quality_contracts(self) -> None:
        tables = {contract.table for contract in contracts() if contract.layer == "gold"}

        self.assertEqual(
            tables,
            {
                "ahon.gold.dim_lgu",
                "ahon.gold.dim_cmci_indicator",
                "ahon.gold.fact_cmci_indicator",
                "ahon.gold.fact_population",
                "ahon.gold.fact_ldrrmf",
                "ahon.gold.fact_earthquake_event",
            },
        )

    def test_gold_setup_matches_the_supported_table_contracts(self) -> None:
        ddl = (SQL_ROOT / "00_setup" / "07_gold_setup.sql").read_text(
            encoding="utf-8"
        )
        expected_tables = {
            contract.table
            for contract in contracts()
            if contract.layer == "gold"
        }

        for table in expected_tables:
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", ddl)

        mirrored_ddl = (REPOSITORY_ROOT / "tests" / "sql" / "00_setup" / "07_gold_setup.sql").read_text(
            encoding="utf-8"
        )
        self.assertEqual(ddl, mirrored_ddl)

    def test_gold_ddl_columns_match_quality_contracts(self) -> None:
        ddl = (SQL_ROOT / "00_setup" / "07_gold_setup.sql").read_text(
            encoding="utf-8"
        )
        for contract in contracts():
            if contract.layer != "gold":
                continue
            match = re.search(
                rf"CREATE TABLE IF NOT EXISTS {re.escape(contract.table)} \((.*?)\)\s+USING DELTA",
                ddl,
                flags=re.DOTALL,
            )
            self.assertIsNotNone(match, contract.table)
            ddl_columns = [
                line.strip().split()[0].strip("`,")
                for line in match.group(1).splitlines()
                if line.strip()
            ]
            self.assertEqual(ddl_columns, list(contract.columns), contract.table)

    def test_population_fact_keeps_city_municipality_source_grain(self) -> None:
        builder = (SQL_ROOT / "gold" / "build_gold.py").read_text(encoding="utf-8")

        self.assertIn('F.lit("UNMATCHED")', builder)
        self.assertIn('alias("utilization_rate")', builder)
        self.assertIn('F.col("geographic_level") == "City/Municipality"', builder)
        self.assertIn('alias("psgc_code")', builder)
        self.assertIn('"ahon.gold.fact_population"', builder)

    def test_ldrrmf_gold_is_scoped_to_cities_and_municipalities(self) -> None:
        builder = (SQL_ROOT / "gold" / "build_gold.py").read_text(encoding="utf-8")

        self.assertIn('F.col("lgu_type").isin("City", "Municipality")', builder)
        self.assertIn("all excluded rows remain in Silver", builder)

