# 0005: Bronze quality results and source Silver mappings

- **Status:** Accepted
- **Date:** 2026-10-09

## Context

The repository has multiple Bronze sources but no shared persisted quality-result contract. Several documented Silver mappings were also missing, while some proposed risk and preparedness measures remain undefined.

## Decision

- Bronze validation is read-only, aggregates each source table once per run, and appends per-rule counts to `ahon.monitoring.dq_result`.
- Missing provenance, malformed row hashes, empty source tables, and schema drift block the validation job. Source-value anomalies and duplicate candidates are warnings; validation never removes source rows.
- Silver applies source-level typing and mapping, carries available lineage, and remains separate from risk/preparedness score calculation and unresolved geographic matching.
- Silver snapshots use overwrite when the source table is small and the entire Bronze history is the source of truth; CMCI and PHIVOLCS retain their documented merge behavior.

## Why

This keeps raw evidence auditable, produces actionable counts with a small number of table scans, and lets downstream modeling proceed without inventing metrics or geographic matches.

## Consequences

- Operators must create `ahon.monitoring.dq_result` before running the validator.
- Quality checks append history by `run_id`; repeated attempts do not overwrite earlier results.
- Dataset dictionaries document each Silver grain, key, and source mapping.
- Existing PSA tables with legacy provenance names require the one-time migration script before the aligned loader and validator can run.
- That PSA migration enables Delta column mapping and upgrades the Delta protocol as required; workspace readers must support the resulting table feature.
- At the time this decision was accepted, risk/preparedness measures, barangay population coverage, and LDRRMF/PHIVOLCS geographic matching remained open.
- The population-grain item was resolved by [0006](0006-city-municipality-analysis-grain.md); risk/preparedness definitions and LDRRMF/PHIVOLCS matching remain open.
