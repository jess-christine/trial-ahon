from __future__ import annotations

import ast
import json
import re
import sqlite3
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock

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



class CMCIRecoveryTests(TestCase):
    def test_missing_pillar_uses_serverless_supported_create_mode(self):
        source = (REPOSITORY_ROOT / "src/sql/02_silver_clean/cmci_indicator_parse.py").read_text()
        function = next(node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name == "merge_pillar")
        namespace = {"SparkSession": object}
        exec(compile(ast.Module(body=[function], type_ignores=[]), "parser", "exec"), namespace)  # noqa: S102 - trusted repository function, isolated from Spark imports
        spark, frame = MagicMock(), MagicMock()
        spark.catalog.tableExists.return_value = False
        namespace["merge_pillar"](spark, "silver.cmci_resiliency", frame)
        frame.write.format.return_value.mode.assert_called_once_with("error")

    def test_recovery_checks_precede_pillar_writes(self):
        source = (REPOSITORY_ROOT / "src/sql/02_silver_clean/cmci_indicator_parse.py").read_text()
        self.assertLess(source.index("if broken_batches:"), source.index("merge_pillar(spark, table_name, frame)"))
        self.assertIn('F.col("reviewed")', source)
        self.assertIn('if expected_rows == 0:', source)
        self.assertNotIn('.cache()', source)
        ingest = (REPOSITORY_ROOT / "src/sql/01_bronze_ingest/cmci_batch_ingest.py").read_text()
        self.assertIn('["batch_id", "psgc_code", "indicator_label", "year", "response_hash"]', ingest)
        self.assertIn('"ingestion_timestamp": ingested_at.isoformat()', ingest)
        self.assertIn('F.col("returned_value_count")', ingest)
        self.assertNotIn('EXPECTED_APPROVED_MAPPING_COUNT =', ingest)


class PSGCHierarchyTests(TestCase):
    def test_hierarchy_uses_same_version_full_parent_codes_without_fanout(self):
        sql = (REPOSITORY_ROOT / "src/sql/datasets/psgc/silver/psgc_clean.sql").read_text()
        schema = sql.split("(", 1)[1].split(")", 1)[0]
        columns = [line.strip().split()[0] for line in schema.splitlines() if line.strip()]
        with sqlite3.connect(":memory:") as conn:
            conn.create_function("concat", -1, lambda *values: "".join(values))
            conn.create_function("regexp", 2, lambda pattern, value: bool(value and re.fullmatch(pattern, value)))
            conn.execute("CREATE TABLE bronze_psgc (" + ",".join(columns) + ")")
            records = [
                ("0100000000", "Reg", "Q2_2024"),
                ("0102800000", "Prov", "Q2_2024"),
                ("0102800000", "Prov", "Q2_2024"),
                ("0102801000", "Mun", "Q2_2024"),
                ("0102801001", "Bgy", "Q2_2024"),
                ("0102801000", "Mun", "Q4_2023"),
            ]
            for code, level, version in records:
                values = {"psgc_code": code, "geographic_level": level, "version": version,
                          "region_code": "1", "province_code": "28", "municipality_code": "1", "barangay_code": "0"}
                conn.execute("INSERT INTO bronze_psgc VALUES (" + ",".join("?" for _ in columns) + ")", [values.get(c) for c in columns])
            query = sql.split("INSERT OVERWRITE TABLE ahon.silver.psgc_clean", 1)[1].replace("ahon.bronze.psgc", "bronze_psgc").replace(" RLIKE ", " REGEXP ")
            rows = [dict(zip(columns, row)) for row in conn.execute(query)]
        self.assertEqual(len(rows), len(records))
        city = next(r for r in rows if r["psgc_code"] == "0102801000" and r["version"] == "Q2_2024")
        self.assertEqual(city["region_code"], "0100000000")
        self.assertEqual(city["province_code"], "0102800000")
        self.assertEqual(city["municipality_code"], "0102801000")
        self.assertIsNone(city["barangay_code"])
        historical = next(r for r in rows if r["version"] == "Q4_2023")
        self.assertIsNone(historical["region_code"])
        self.assertIsNone(historical["province_code"])
        barangay = next(r for r in rows if r["geographic_level"] == "Bgy")
        self.assertEqual(barangay["barangay_code"], "0102801001")


class BoundaryContractTests(TestCase):
    def test_live_wkb_contract_is_shared_by_loader_and_consumers(self):
        for relative in ("src/sql/platinum/load_lgu_boundaries.py", "src/sql/gold/build_gold.py", "src/sql/platinum/build_platinum.py"):
            source = (REPOSITORY_ROOT / relative).read_text()
            self.assertIn("try_to_geometry(unhex(boundary_wkb_hex))", source)
            self.assertNotIn("try_to_geometry(boundary_geojson)", source)
            if not relative.endswith("load_lgu_boundaries.py"):
                self.assertIn("st_setsrid(try_to_geometry(unhex(boundary_wkb_hex)), 4326)", source)
        loader = (REPOSITORY_ROOT / "src/sql/platinum/load_lgu_boundaries.py").read_text()
        gold = (REPOSITORY_ROOT / "src/sql/gold/build_gold.py").read_text()
        self.assertIn('.select("id", "psgc_code")\n        .groupBy("id")', gold)
        self.assertIn("$.properties.geometry_wkb_hex", loader)
        self.assertIn('"boundary_has_mapped_codes"', loader)
        self.assertIn('if any(failures and severity == "BLOCKING"', loader)
        bundle = (REPOSITORY_ROOT / "databricks.yml").read_text()
        self.assertNotIn("boundary_code_property: PSGC", bundle)
        self.assertIn("boundary_code_property: psgc_code", bundle)


class EarthquakeSilverDuplicateTests(TestCase):
    def evaluate_candidate(self, observations):
        sql = (REPOSITORY_ROOT / "src/sql/datasets/philvolcs_earthquake/silver/philvolcs_earthquake_clean.sql").read_text()
        columns = ("id", "event_time", "latitude", "longitude", "depth", "magnitude", "location_description", "month", "year", "_source_name", "_source_ref", "_ingested_at", "_batch_id", "_row_hash")
        with sqlite3.connect(":memory:") as conn:
            conn.create_function("named_struct", -1, lambda *values: json.dumps(values))
            def fail(message):
                raise ValueError(message)
            conn.create_function("raise_error", 1, fail)
            conn.execute("CREATE TABLE phivolcs_event_candidate (" + ",".join(columns) + ")")
            for key, location, loaded, batch, row_hash in observations:
                conn.execute("INSERT INTO phivolcs_event_candidate VALUES (" + ",".join("?" for _ in columns) + ")",
                             (key, "2026-01-01", 10.0, 120.0, 2.0, 3.0, location, 1, 2026, "source", "ref", loaded, batch, row_hash))
            conflict_sql = sql.split("CREATE OR REPLACE TEMP VIEW phivolcs_event_conflicts AS", 1)[1].split(";", 1)[0]
            conn.execute("CREATE TEMP VIEW phivolcs_event_conflicts AS " + conflict_sql)
            ranking = sql.split("INSERT OVERWRITE TABLE ahon.silver.philvolcs_earthquake_data_clean", 1)[1]
            rows = [dict(zip(columns, row)) for row in conn.execute(ranking)]
        return rows

    def test_identical_events_select_latest_lineage_and_retain_history(self):
        rows = self.evaluate_candidate([(1, "same", "2026-01-01", "a", "1"),
                                        (1, "same", "2026-01-02", "a", "2"),
                                        (2, "historic", "2025-01-01", "a", "3")])
        self.assertEqual(len(rows), 2)
        self.assertEqual(next(row for row in rows if row["id"] == 1)["_row_hash"], "2")
        self.assertEqual(rows, self.evaluate_candidate([(r["id"], r["location_description"], r["_ingested_at"], r["_batch_id"], r["_row_hash"]) for r in rows]))

    def test_lineage_ties_have_deterministic_order(self):
        observations = [(1, "same", "2026-01-01", "a", "1"), (1, "same", "2026-01-01", "b", "2")]
        self.assertEqual(self.evaluate_candidate(observations), self.evaluate_candidate(list(reversed(observations))))

    def test_conflicting_event_attributes_block_selection(self):
        with self.assertRaises(sqlite3.OperationalError):
            self.evaluate_candidate([(1, "original", "2026-01-01", "a", "1"),
                                     (1, "different", "2026-01-02", "b", "2")])
