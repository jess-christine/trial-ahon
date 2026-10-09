# Dataset processing runbook

## Initial setup

Run `src/sql/00_setup/00_catalog_schema_setup.sql`, `01_bronze_setup.sql`, `02_monitoring_setup.sql`, and the applicable reference setup scripts in order. Run `03_migrate_psa_bronze_provenance.sql` only when upgrading an existing PSA Bronze table with legacy provenance names; fresh tables already use the standard names. This migration enables Delta column mapping, which raises the table protocol; confirm every reader supports the Delta version required by the workspace before running it.

For automated execution, validate/deploy `databricks.yml` and `resources/ahon_pipeline.yml` using the [Databricks bundle runbook](databricks-bundle.md). Before running, land the active PSGC workbook, reviewed CMCI locality mapping, and approved approximate Geoportal boundary FeatureCollection in the configured reference source volume. The boundary PSGC property name is a required deployment variable.

## Processing sequence

1. Land or fetch source files using the dataset extract step.
2. Run the dataset Bronze loader. Bronze retains source values and adds provenance; it does not standardize or filter quality findings.
3. Run `src/sql/monitoring/bronze_quality.py`. Resolve `FAIL` outcomes before continuing. Review and record `WARN` counts; warnings preserve source evidence.
4. Run the dataset Silver SQL. Run `src/sql/02_silver_clean/cmci_batch_status.sql` for batch status, followed by `src/sql/02_silver_clean/cmci_indicator_parse.py` for CMCI's five pillar tables.
5. Review the dataset's data-dictionary page for keys, source lineage, limitations, and expected coverage before joining sources in Gold.
6. Load the approximate boundary GeoJSON with `src/sql/platinum/load_lgu_boundaries.py`, then run Gold setup and `src/sql/gold/build_gold.py`. Both PSA and BLGF use exact LGU + province name matching; ambiguous/unmatched rows keep null PSGC. LDRRMF province rows remain in Silver and are counted as an intentional Gold exclusion. PHIVOLCS event matching requires exactly one containing approximate polygon.
7. Run `src/sql/monitoring/silver_gold_quality.py`. Resolve blocking failures and review warning counts before using Gold outputs.
8. Run `src/sql/platinum/build_platinum.py` for experimental v1 risk, preparedness, vulnerability, and CMCI priority snapshots. Review its coverage warnings and the limitations in the [Platinum data dictionary](../architecture/data-dictionary/platinum_analytics.md) before communicating results.

## Safe reruns

Bronze validation is read-only and appends a new run ID each time. BLGF and PSGC Silver snapshots are rebuilt from their Bronze source while preserving duplicate rows. PSA uses its documented `geographic_location + census_year` merge key. CMCI uses Delta `MERGE` on its documented `psgc_code + year` key and selects the most recent complete source batch for that key. PHIVOLCS Silver retains one identical event per documented key under decision 0009, preserving history and blocking conflicting attributes.

Platinum metrics are experimental and use the owner-approved v1 assumptions documented in the [Platinum data dictionary](../architecture/data-dictionary/platinum_analytics.md). Do not present them as calibrated operational thresholds. Keep approximate-boundary matches labeled with their limitation. Do not match LDRRMF or events by free-text names or locations outside these approved rules.

## Recovering CMCI without a source reload

Run ingestion with `--only cmci_ingestion` to repair any HTML-only/partial row
Bronze batch. Run medallion with `--only cmci_silver_status,cmci_silver_parse` to
refresh pillars; unchanged approved source coverage causes no portal indicator
requests. Inspect persisted `complete_html_batches_have_all_indicator_rows`,
`complete_batch_lgu_year_count` and `silver_indicator_to_gold_fact_row_count`
results before relying on Gold. The 2026-10-09 dev repair recovered 17,886 rows in
each pillar and 626,010 Gold indicator rows, with no duplicate LGU-year grains.

PHIVOLCS Silver now follows decision 0009: combine valid current Bronze with
retained history, reject conflicting event attributes, and keep one identical
event per existing key using deterministic ingestion/batch/hash ordering. Run
`silver_phivolcs`, Gold, Silver/Gold validation and Platinum in dependency order.
Do not delete historical Silver events merely because current Bronze is smaller.
Gold still rejects any remaining duplicate event keys before spatial processing.
