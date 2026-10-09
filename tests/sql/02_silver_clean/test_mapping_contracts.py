from __future__ import annotations

from pathlib import Path
from unittest import TestCase

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

MAPPINGS = {
    "src/sql/datasets/blgf_ldrrmf_annual_lgu/silver/blgf_ldrrmf_annual_lgu_clean.sql": (
        "ahon.bronze.blgf_ldrrmf_annual_lgu",
        "ahon.silver.blgf_ldrrmf_annual_lgu_clean",
    ),
    "src/sql/datasets/psa_population/silver/psa_population_clean.sql": (
        "ahon.bronze.psa_population_raw",
        "ahon.silver.psa_population_clean",
    ),
    "src/sql/datasets/psgc/silver/psgc_clean.sql": (
        "ahon.bronze.psgc",
        "ahon.silver.psgc_clean",
    ),
    "src/sql/datasets/philvolcs_earthquake/silver/philvolcs_earthquake_clean.sql": (
        "ahon.bronze.philvolcs_earthquake_data",
        "ahon.silver.philvolcs_earthquake_data_clean",
    ),
    "src/sql/02_silver_clean/cmci_batch_status.sql": (
        "ahon.bronze.cmci_raw_indicator_batch_html",
        "ahon.silver.cmci_ingestion_batch_clean",
    ),
}


class SilverMappingTests(TestCase):
    def test_sql_silver_mappings_read_the_documented_bronze_table(self) -> None:
        for relative_path, (bronze_table, silver_table) in MAPPINGS.items():
            sql = (REPOSITORY_ROOT / relative_path).read_text(encoding="utf-8")

            self.assertIn(bronze_table, sql)
            self.assertIn(silver_table, sql)

    def test_every_cmci_pillar_is_built_from_the_indicator_bronze_table(self) -> None:
        parser = (
            REPOSITORY_ROOT / "src/sql/02_silver_clean/cmci_indicator_parse.py"
        ).read_text(encoding="utf-8")

        self.assertIn('BRONZE_TABLE = f"{CATALOG}.bronze.cmci_raw_indicator"', parser)
        self.assertIn('SILVER_PREFIX = f"{CATALOG}.silver.cmci_"', parser)
        self.assertIn(
            "for pillar_name, indicators in INDICATORS_BY_PILLAR.items()", parser
        )
        self.assertIn('pillar_name.lower().replace(" ", "_")', parser)

        dictionary = (
            REPOSITORY_ROOT / "docs/architecture/data-dictionary/cmci_bronze.md"
        ).read_text(encoding="utf-8")
        for pillar in (
            "economic_dynamism",
            "government_efficiency",
            "infrastructure",
            "resiliency",
            "innovation",
        ):
            self.assertIn(f"ahon.silver.cmci_{pillar}", dictionary)

