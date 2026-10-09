# Dataset processing runbook

## Initial setup

Run `src/sql/00_setup/00_catalog_schema_setup.sql`, `01_bronze_setup.sql`, `02_monitoring_setup.sql`, and the applicable reference setup scripts in order. Run `03_migrate_psa_bronze_provenance.sql` only when upgrading an existing PSA Bronze table with legacy provenance names; fresh tables already use the standard names. This migration enables Delta column mapping, which raises the table protocol; confirm every reader supports the Delta version required by the workspace before running it.

## Processing sequence

1. Land or fetch source files using the dataset extract step.
2. Run the dataset Bronze loader. Bronze retains source values and adds provenance; it does not standardize or filter quality findings.
3. Run `src/sql/monitoring/bronze_quality.py`. Resolve `FAIL` outcomes before continuing. Review and record `WARN` counts; warnings preserve source evidence.
4. Run the dataset Silver SQL. Run `src/sql/02_silver_clean/cmci_batch_status.sql` for batch status, followed by `src/sql/02_silver_clean/cmci_indicator_parse.py` for CMCI's five pillar tables.
5. Review the dataset's data-dictionary page for keys, source lineage, limitations, and expected coverage before joining sources in Gold.
6. Run `src/sql/00_setup/07_gold_setup.sql`, then `src/sql/gold/build_gold.py`. The build uses only cities and municipalities for the LGU facts; PSA population uses exact LGU + province text matching and keeps unmatched rows with null PSGC. LDRRMF province lines remain in Silver and are counted as an intentional Gold exclusion.
7. Run `src/sql/monitoring/silver_gold_quality.py`. Resolve blocking failures and review warning counts before using Gold outputs.

## Safe reruns

Bronze validation is read-only and appends a new run ID each time. BLGF and PSGC Silver snapshots are rebuilt from their Bronze source while preserving duplicate rows. PSA uses its documented `geographic_location + census_year` merge key. CMCI uses Delta `MERGE` on its documented `psgc_code + year` key and selects the most recent complete source batch for that key. PHIVOLCS Silver uses its documented event key and retains all valid source observations.

Do not use the Silver outputs to calculate risk or preparedness scores until the open definitions in [the data model](../architecture/data-model.md) are approved. Do not join LDRRMF or PHIVOLCS to LGUs using names or free-text locations without a reviewed mapping rule.
