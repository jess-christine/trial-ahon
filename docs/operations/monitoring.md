# Bronze quality monitoring

## Run

1. Run the setup scripts, including `src/sql/00_setup/02_monitoring_setup.sql`, to create `ahon.monitoring.dq_result`.
2. Ensure all implemented Bronze tables exist and the PSA provenance migration has been applied where required.
3. Run `src/sql/monitoring/bronze_quality.py` on Databricks compute with read access to the Bronze tables and append access to `ahon.monitoring.dq_result`.
4. After Silver and Gold builds, run `src/sql/monitoring/silver_gold_quality.py`; it records the layer in each result's `details` JSON and validates keys, types/ranges, source-to-target counts, and Gold references.

Each validator aggregates its table once for configured rules and appends results to the same table. `run_id` groups one validation attempt; `checked_at`, time bounds, observed period/geography counts, and batch counts make freshness and coverage visible. The layer validator adds explicit source/target count reconciliation and CMCI foreign-key checks. Retried runs append a new `run_id` and leave earlier evidence intact.

## Outcomes

- `PASS`: zero rows violated the rule.
- `WARN`: source-quality anomalies were counted and retained; the job continues.
- `FAIL`: a blocking rule failed. Current blocking rules cover missing provenance, malformed SHA-256 hashes, empty Bronze tables, and schema drift. Results are written before the job exits unsuccessfully.

Source-value anomalies such as missing measures, invalid ranges, inconsistent totals, unknown row types, duplicate natural-key candidates, unmatched geography, null CMCI scores, and additional source columns are warnings. Missing required columns, declared Silver/Gold key duplicates, broken Gold foreign keys, and source/target count mismatches block because downstream outputs would be unreliable. Missing required columns block because checks cannot be evaluated reliably; additional columns are recorded without blocking, so source schema evolution stays visible while Bronze remains untouched. Exact key duplicate policy remains source-specific and is intentionally not used to discard Bronze rows.

Rule results are stored in `ahon.monitoring.dq_result`. Query by the `run_id` printed by the job; use `table_name`, `rule_name`, `failed_count`, `severity`, and `details` to locate affected Bronze rows. A row can be traced from Bronze by its source and batch provenance.
