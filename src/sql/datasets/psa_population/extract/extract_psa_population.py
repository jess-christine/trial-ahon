# Databricks notebook source
"""Extract PSA 2024 census population data from the PXWeb API into a UV volume."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import requests

# COMMAND ----------

# PXWeb API endpoint for the 2024 census table:
# Total Population, Urban Population, and Percent Urban by geographic location
API_URL = "https://openstat.psa.gov.ph/PXWeb/api/v1/en/DB/1A/PO_2024/0241A6DPUP1.px"

# Destination: the source volume, one folder per dataset (naming standard)
CATALOG = os.environ.get("AHON_CATALOG", "ahon")
VOLUME_ROOT = Path(os.environ.get("AHON_SOURCE_VOLUME", f"/Volumes/{CATALOG}/reference/source"))
DATASET_NAME = "psa_population"
TARGET_PATH = VOLUME_ROOT / DATASET_NAME / "2024_population_urban.csv"

REQUEST_TIMEOUT_SECONDS = 60

# POST query: select all geographic locations and all parameters, return as CSV
QUERY_BODY: dict[str, Any] = {
    "query": [
        {
            "code": "Geographic Location",
            "selection": {"filter": "all", "values": ["*"]},
        },
        {
            "code": "Parameter",
            "selection": {"filter": "all", "values": ["*"]},
        },
    ],
    "response": {"format": "csv"},
}

# COMMAND ----------


def fetch_csv() -> requests.Response:
    """POST the PXWeb query and return the validated CSV response."""
    resp = requests.post(
        API_URL,
        json=QUERY_BODY,
        headers={"Accept": "text/csv"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    return resp


def save_csv(content: bytes, target: Path) -> None:
    """Write the raw CSV bytes to the target path, creating folders as needed."""
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)


def main() -> None:
    """Download the dataset, save it to the volume, and print a summary."""
    resp = fetch_csv()
    save_csv(resp.content, TARGET_PATH)

    lines = resp.text.strip().splitlines()
    print(f"Saved {len(resp.content):,} bytes to {TARGET_PATH}")
    print(f"Lines: {len(lines)}")
    print(f"Header: {lines[0] if lines else '<empty response>'}")


# COMMAND ----------

if __name__ == "__main__":
    main()
