from __future__ import annotations

from pathlib import Path
from unittest import TestCase

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


class BundleContractTests(TestCase):
    def test_bundle_includes_ordered_pipeline_resources(self) -> None:
        bundle = (REPOSITORY_ROOT / "databricks.yml").read_text(encoding="utf-8")
        job = (REPOSITORY_ROOT / "resources" / "ahon_pipeline.yml").read_text(encoding="utf-8")
        expected_task_keys = (
            "setup_catalog",
            "validate_bronze",
            "load_geoportal_boundaries",
            "build_gold",
            "validate_silver_gold",
            "build_platinum",
        )

        self.assertIn("resources/*.yml", bundle)
        positions = [job.index(f"task_key: {task}") for task in expected_task_keys]
        self.assertEqual(positions, sorted(positions))
        self.assertIn("AHON_CATALOG: ${var.catalog}", job)
        self.assertIn("spark_version: ${var.spark_version}", job)
        self.assertIn("max_retries: 1", job)
        self.assertIn("psgc_secret_scope: ${var.psgc_secret_scope}", job)

    def test_bundle_task_files_exist(self) -> None:
        job = (REPOSITORY_ROOT / "resources" / "ahon_pipeline.yml").read_text(encoding="utf-8")
        for referenced_path in (
            "src/sql/common/run_sql_file.py",
            "src/sql/platinum/load_lgu_boundaries.py",
            "src/sql/platinum/build_platinum.py",
            "src/sql/gold/build_gold.py",
            "src/sql/monitoring/bronze_quality.py",
        ):
            self.assertTrue((REPOSITORY_ROOT / referenced_path).is_file(), referenced_path)
            self.assertIn(referenced_path, job.replace("../", ""))

    def test_silver_tasks_wait_for_successful_bronze_validation(self) -> None:
        job = (REPOSITORY_ROOT / "resources" / "ahon_pipeline.yml").read_text(encoding="utf-8")
        for task_key in (
            "silver_psa",
            "silver_blgf",
            "silver_psgc",
            "silver_phivolcs",
            "cmci_silver_status",
        ):
            task = job.split(f"task_key: {task_key}", 1)[1].split("- task_key:", 1)[0]
            self.assertIn("{task_key: validate_bronze}", task, task_key)

    def test_psgc_workspace_task_uses_databricks_python_notebook_source(self) -> None:
        notebook = (REPOSITORY_ROOT / "src/sql/datasets/psgc/psgc_api.py").read_text(
            encoding="utf-8"
        )
        self.assertTrue(notebook.startswith("# Databricks notebook source"))
