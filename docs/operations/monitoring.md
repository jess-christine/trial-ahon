# Bronze quality monitoring

## Run

1. Run the setup scripts, including `src/sql/00_setup/02_monitoring_setup.sql`, to create `ahon.monitoring.dq_result`.
2. Ensure all implemented Bronze tables exist and the PSA provenance migration has been applied where required.
3. Run `src/sql/monitoring/bronze_quality.py` on Databricks compute with read access to the Bronze tables and append access to `ahon.monitoring.dq_result`.
4. After Silver and Gold builds, run `src/sql/monitoring/silver_gold_quality.py`; it records the layer in each result's `details` JSON and validates keys, types/ranges, source-to-target counts, declared Gold grains, and Gold references.
5. The Platinum builder records output-grain, current-run, and category checks before replacing snapshots. Review its results alongside the coverage warnings; a failed blocking check prevents publication.

Each validator aggregates its table once for configured rules and appends results to the same table. `run_id` groups one validation attempt; `checked_at`, time bounds, observed period/geography counts, and batch counts make freshness and coverage visible. The layer validator adds explicit source/target count reconciliation and CMCI foreign-key checks. Retried runs append a new `run_id` and leave earlier evidence intact.

## Outcomes

- `PASS`: zero rows violated the rule.
- `WARN`: source-quality anomalies were counted and retained; the job continues.
- `FAIL`: a blocking rule failed. Current blocking rules cover missing provenance, malformed SHA-256 hashes, empty Bronze tables, and schema drift. Results are written before the job exits unsuccessfully.

Source-value anomalies such as missing measures, invalid ranges, inconsistent totals, unknown row types, duplicate natural-key candidates, unmatched geography, null CMCI scores, approximate-boundary code coverage, and additional source columns are warnings. Missing required columns, malformed boundary geometry, declared Silver/Gold key duplicates, duplicate matched PSA LGU/year rows in Platinum inputs, broken Gold foreign keys, incorrect calculable utilization, and source/target count mismatches block because downstream outputs would be unreliable. LDRRMF fact grain is an original report line; Platinum safely aggregates its monetary components by matched LGU/year before computing utilization. Platinum also blocks duplicate output grain, incorrect run IDs, invalid non-null categories, and mismatched formulas/null behavior. Missing required columns block because checks cannot be evaluated reliably; additional columns are recorded without blocking, so source schema evolution stays visible while Bronze remains untouched. Exact key duplicate policy remains source-specific and is intentionally not used to discard Bronze rows.

Rule results are stored in `ahon.monitoring.dq_result`. Query by the `run_id` printed by the job; use `table_name`, `rule_name`, `failed_count`, `severity`, and `details` to locate affected Bronze rows. A row can be traced from Bronze by its source and batch provenance.
