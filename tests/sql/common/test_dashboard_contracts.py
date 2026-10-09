"""Dashboard datasets, widget references, layout and approved score contracts."""
from __future__ import annotations

import json
from pathlib import Path
from unittest import TestCase

ROOT = Path(__file__).resolve().parents[3]
DASHBOARD = json.loads((ROOT / "resources/ahon_risk_preparedness.lvdash.json").read_text(encoding="utf-8"))


class DashboardContracts(TestCase):
    def test_every_business_question_has_populated_page(self):
        self.assertEqual(len(DASHBOARD["pages"]), 4)
        for page in DASHBOARD["pages"]:
            self.assertGreaterEqual(len(page["layout"]), 8)

    def test_all_widget_dataset_fields_and_encodings_resolve(self):
        datasets = {d["name"]: d["config"] for d in DASHBOARD["datasets"]}
        names = set()
        for page in DASHBOARD["pages"]:
            for item in page["layout"]:
                widget = item["widget"]
                self.assertNotIn(widget["name"], names)
                names.add(widget["name"])
                for query in widget.get("queries", []):
                    config = datasets[query["query"]["datasetName"]]
                    dimensions = {d["name"] for d in config["dimensions"]}
                    measures = {f"measure({m['name']})" for m in config["measures"]}
                    fields = {f["name"] for f in query["query"]["fields"]}
                    self.assertLessEqual(fields, dimensions | measures)
                    def check(value, fields=fields):
                        if isinstance(value, dict):
                            if "fieldName" in value:
                                self.assertIn(value["fieldName"], fields)
                            for child in value.values():
                                check(child)
                        elif isinstance(value, list):
                            for child in value:
                                check(child)
                    check(widget["spec"]["encodings"])

    def test_no_widgets_overlap(self):
        for page in DASHBOARD["pages"]:
            positions = [(e["widget"]["name"], e["position"]) for e in page["layout"]]
            for index, (name, a) in enumerate(positions):
                self.assertLessEqual(a["x"] + a["width"], 12)
                for other, b in positions[index + 1:]:
                    overlap = (a["x"] < b["x"] + b["width"] and b["x"] < a["x"] + a["width"]
                               and a["y"] < b["y"] + b["height"] and b["y"] < a["y"] + a["height"])
                    self.assertFalse(overlap, f"{name} overlaps {other}")

    def test_scores_are_not_imputed_and_ranking_is_explicit(self):
        sources = {d["name"]: d["config"]["source"] for d in DASHBOARD["datasets"]}
        self.assertIn("'Mun'", sources["lgu_risk_prep"])
        self.assertIn("r.run_id = p.run_id", sources["lgu_risk_prep"])
        self.assertIn("r.run_id = v.run_id", sources["lgu_risk_prep"])
        self.assertNotIn("ahon_dev.", json.dumps(DASHBOARD))
        for name in ["top_risk", "top_gap", "highest_priority"]:
            self.assertIn("ROW_NUMBER()", sources[name])
            self.assertIn("<= 20", sources[name])
        self.assertIn("preparedness_gap DESC", sources["top_gap"])
        self.assertIn("No calculated highest-priority cohort", sources["indicator_priorities"])
