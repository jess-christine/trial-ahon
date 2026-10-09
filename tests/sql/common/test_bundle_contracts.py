from __future__ import annotations

import json
from pathlib import Path
from unittest import TestCase

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


class BundleContractTests(TestCase):
    def test_bundle_defines_separate_ingestion_and_medallion_jobs(self) -> None:
        bundle = (REPOSITORY_ROOT / "databricks.yml").read_text(encoding="utf-8")
        job = (REPOSITORY_ROOT / "resources" / "ahon_pipeline.yml").read_text(encoding="utf-8")
        self.assertIn("resources/*.yml", bundle)
        self.assertIn("ahon_ingestion:", job)
        self.assertIn("ahon_medallion:", job)
        self.assertIn("name: ahon-ingestion-${bundle.target}", job)
        self.assertIn("name: ahon-medallion-${bundle.target}", job)
        self.assertIn("job_id: ${resources.jobs.ahon_ingestion.id}", job)
        config = json.loads(yaml.safe_load(bundle)["variables"]["runtime_config"]["default"])
        self.assertEqual(config["AHON_CATALOG"], "${var.catalog}")
        self.assertEqual(config["AHON_REPOSITORY_ROOT"], "${workspace.file_path}")
        self.assertIn("environment_version: ${var.environment_version}", job)
        self.assertIn("max_retries: 1", job)
        self.assertIn("psgc_secret_scope: ${var.psgc_secret_scope}", job)
        for assignment in (
            "AHON_PSA_API_URL: ${var.psa_api_url}",
            "AHON_PSA_CENSUS_YEAR: ${var.psa_census_year}",
            "AHON_PHIVOLCS_BASE_URL: ${var.phivolcs_base_url}",
            "AHON_BLGF_DATASET_REPO_ID: ${var.blgf_dataset_repo_id}",
            "AHON_CMCI_PROCESS_URL: ${var.cmci_process_url}",
            "AHON_CMCI_YEARS: ${var.cmci_years}",
        ):
            name, value = assignment.split(": ", 1)
            self.assertEqual(config[name], value)
        self.assertIn("psgc_periods: ${var.psgc_periods}", job)

    def test_ingestion_owns_source_and_bronze_quality_tasks(self) -> None:
        job = (REPOSITORY_ROOT / "resources" / "ahon_pipeline.yml").read_text(encoding="utf-8")
        ingestion, medallion = job.split("    ahon_medallion:", 1)
        for task_key in (
            "setup_catalog", "load_active_lgu_reference", "extract_psa", "bronze_psa",
            "extract_blgf", "bronze_blgf", "extract_phivolcs", "bronze_phivolcs",
            "extract_psgc", "cmci_ingestion", "load_geoportal_boundaries", "validate_bronze",
        ):
            self.assertIn(f"task_key: {task_key}", ingestion)
        self.assertNotIn("task_key: build_gold", ingestion)
        for task_key in ("silver_psa", "silver_blgf", "silver_psgc", "silver_phivolcs", "cmci_silver_status"):
            task = medallion.split(f"task_key: {task_key}", 1)[1].split("- task_key:", 1)[0]
            parsed = yaml.safe_load(job)["resources"]["jobs"]["ahon_medallion"]["tasks"]
            selected = next(t for t in parsed if t["task_key"] == task_key)
            self.assertEqual(selected["depends_on"], [{"task_key": "run_ingestion"}])
            self.assertNotIn("{task_key: validate_bronze}", task, task_key)

    def test_medallion_job_runs_ingestion_before_downstream_layers(self) -> None:
        job = (REPOSITORY_ROOT / "resources" / "ahon_pipeline.yml").read_text(encoding="utf-8")
        _, medallion = job.split("    ahon_medallion:", 1)
        expected = (
            "run_ingestion", "silver_psa", "silver_blgf", "silver_psgc", "silver_phivolcs",
            "cmci_silver_status", "cmci_silver_parse", "build_gold",
            "validate_silver_gold", "build_platinum",
        )
        positions = [medallion.index(f"task_key: {task}") for task in expected]
        self.assertEqual(positions, sorted(positions))
        tasks = yaml.safe_load(job)["resources"]["jobs"]["ahon_medallion"]["tasks"]
        selected = next(t for t in tasks if t["task_key"] == "validate_silver_gold")
        self.assertEqual(selected["depends_on"], [{"task_key": "build_gold"}])

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
            self.assertIn(referenced_path, json.dumps(yaml.safe_load(job)).replace("../", ""))

    def test_psgc_workspace_task_uses_databricks_python_notebook_source(self) -> None:
        notebook = (REPOSITORY_ROOT / "src/sql/datasets/psgc/psgc_api.py").read_text(
            encoding="utf-8"
        )
        self.assertTrue(notebook.startswith("# Databricks notebook source"))

    def test_all_job_dependencies_exist_and_are_acyclic(self) -> None:
        jobs = yaml.safe_load(
            (REPOSITORY_ROOT / "resources/ahon_pipeline.yml").read_text()
        )["resources"]["jobs"]
        for name, job in jobs.items():
            graph = {
                task["task_key"]: {d["task_key"] for d in task.get("depends_on", [])}
                for task in job["tasks"]
            }
            keys = set(graph)
            self.assertEqual(len(keys), len(job["tasks"]), name)
            for key, dependencies in graph.items():
                self.assertTrue(dependencies <= keys, f"{name}.{key}: {dependencies - keys}")
            done = set()
            while True:
                ready = {key for key, dependencies in graph.items() if dependencies <= done} - done
                if not ready:
                    break
                done.update(ready)
            self.assertEqual(done, keys, f"{name}: cyclic dependencies")

    def test_serverless_tasks_have_environments_config_and_existing_targets(self) -> None:
        jobs = yaml.safe_load(
            (REPOSITORY_ROOT / "resources/ahon_pipeline.yml").read_text()
        )["resources"]["jobs"]
        for job in jobs.values():
            self.assertNotIn("job_clusters", job)
            environments = {e["environment_key"] for e in job["environments"]}
            for task in job["tasks"]:
                self.assertNotIn("job_cluster_key", task)
                if "spark_python_task" in task:
                    self.assertIn(task["environment_key"], environments)
                    params = task["spark_python_task"]["parameters"]
                    self.assertEqual(params[params.index("--config-json") + 1], "${var.runtime_config}")
                    flag = "--sql-file" if "--sql-file" in params else "--python-file"
                    target = REPOSITORY_ROOT / params[params.index(flag) + 1]
                    self.assertTrue(target.is_file())
                    if flag == "--python-file":
                        self.assertFalse(target.read_text(encoding="utf-8").startswith("# Databricks notebook source"))
                if "notebook_task" in task:
                    self.assertEqual(task["notebook_task"]["base_parameters"]["ahon_catalog"], "${var.catalog}")
