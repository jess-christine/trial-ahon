# Databricks Asset Bundle

## Scope and run order

The bundle defines two manual jobs in `resources/ahon_pipeline.yml`. `ahon_ingestion` runs catalog/schema setup, reference loading, source extraction and Bronze loads, boundary loading, then Bronze DQ. `ahon_medallion` starts with a Databricks `run_job_task` that runs `ahon_ingestion` and waits for it to succeed, then runs Silver transforms, Gold, Silver/Gold DQ, and experimental Platinum. Run `ahon_ingestion` alone to refresh and validate source data; run `ahon_medallion` for the full end-to-end pipeline. Both jobs use Serverless environments, permit one concurrent run each, and remain unscheduled. The workspace is Free Edition, so the bundle does not request classic clusters.

## Configure and deploy

Use the Databricks CLI version supported by the workspace. Authenticate with an existing profile and run commands from the repository root:

```powershell
databricks auth login --host <workspace-url> --profile AHON
# Identifiers only; the token value belongs in Databricks Secrets.
$env:BUNDLE_VAR_psgc_secret_scope = "ahon"
$env:BUNDLE_VAR_psgc_secret_key = "psgc_api_token"
$env:BUNDLE_VAR_boundary_code_property = "<actual PSGC property in approved GeoJSON>"
databricks bundle validate -t dev --profile AHON
databricks bundle deploy -t dev --profile AHON
databricks bundle run -t dev ahon_medallion --profile AHON
```

Use `-t prod` only after the development output and mappings are reviewed. The bundle does not include source files or credentials. Configure `environment_version`, `psgc_secret_scope`, `psgc_secret_key`, and `boundary_code_property` for the workspace. Run `ahon_ingestion` independently when only a source refresh is needed; running `ahon_medallion` already invokes ingestion. Catalog and volume paths, source endpoints, PSA census year, PSGC periods, CMCI reporting years, and the PHIVOLCS lookback are bundle variables. Native geospatial SQL functions must be available in the Serverless environment for boundary matching and Platinum proximity.

Before running, ensure:

- The active PSGC publication workbook is landed at the reference source volume root for `load_lgu_master.py`.
- The source-volume folders/files needed by PSA/BLGF/PHIVOLCS are accessible; extractor tasks land source files where their Bronze loaders expect them.
- A reviewed active CMCI locality mapping is present in `reference.cmci_lgu_map`. The job does not create or approve mappings; CMCI fails if reviewed coverage is insufficient.
- The owner-approved approximate city/municipality GeoJSON FeatureCollection is landed at the configured path. Set `boundary_code_property` to the source's actual property containing official PSGC codes; the loader does not infer code fields.
- The deployment identity can create/use the catalog, schemas, volume and tables, and can read the configured secret.

The original GeoPortal inventory reported unavailable downloads. The owner subsequently supplied BetterGov-derived `lgu_spatial.parquet`; prepare it with the existing boundary dictionary procedure and retain the original plus mapping crosswalk in the source volume. See decision 0008 for provenance and partial mapping coverage. The supplied polygons are approximate, and that limitation carries into spatial matches and risk outputs.

## Configuration and reliability

`AHON_CATALOG`, `AHON_SOURCE_VOLUME`, source endpoints, the PSA census year, CMCI reporting years, and PHIVOLCS lookback come from bundle variables. `AHON_REPOSITORY_ROOT` points to the bundle's file path where files are synchronized. SQL task path resolution also uses the workspace task filename or current working directory when Databricks does not define Python's `__file__`, including source-linked deployments. `runtime_config` in `databricks.yml` assembles these settings, including `AHON_BOUNDARY_GEOJSON_PATH` and `AHON_BOUNDARY_CODE_PROPERTY`, into non-secret JSON passed to each Python task. The existing runner sets those values before importing or executing dataset code, so Serverless tasks do not depend on classic-cluster `spark_env_vars` or preview features. PSGC is a notebook task and receives its target catalog explicitly through `ahon_catalog`. PSGC credentials are resolved from a Databricks secret by scope/key identifiers; secret values are not placed in YAML. SQL and Python scripts run through the existing `src/sql/common/run_sql_file.py` using mutually exclusive `--sql-file` / `--python-file` plus `--config-json`. Python dispatch preserves the original script entry point, sibling imports, and error propagation without exposing runner flags to the child parser. Dispatched Python sources must be plain workspace files; only the PSGC notebook retains its Databricks notebook header. This prevents a `.py` source from being uploaded as an extensionless notebook that the runner cannot open. Bundle sync excludes source data, Python bytecode, and cache folders. For SQL the runner validates catalog identifiers, substitutes the configured catalog in qualified names and catalog setup statements, substitutes the source-volume root, and splits statements outside quoted strings/comments. CMCI ingestion and its source profiler share one configured year list.

Bronze boundary ingestion merges on source reference and feature hash, retaining previous source versions while making identical reruns idempotent. Silver, Gold, and Platinum are deterministic snapshots; DQ results append run-scoped outcomes. Source extract tasks may call external endpoints and therefore rely on the repo's existing source-specific retries/validation. A failed upstream task prevents dependent layers from running.

## Monitoring and failure behavior

Bronze and Silver/Gold DQ write to `monitoring.dq_result`. Blocking schema, provenance, duplicate-key, malformed geometry, and referential-integrity failures fail their task after results are recorded. Documented source anomalies, coverage gaps, unmatched geography, and approximate-boundary coverage remain warnings. Platinum reports missing LGU coverage for population, events, and preparedness; null components are preserved and no row is silently filtered from the risk-level snapshot.

Review each task output and query the latest `run_id` in `monitoring.dq_result` before publishing results. Both jobs were deployed to the authenticated development workspace. Live setup, active LGU reference loading, PSA extraction/Bronze (1,744 rows), and BLGF extraction/Bronze tasks succeeded. The PSA loader uses the source-declared Windows-1252 charset and preserves unchanged row provenance on retries. PSGC ingestion and PSA/BLGF Silver transforms also passed live. The supplied boundary artifact is prepared under decision 0008; its live load was submitted but remained queued under the workspace five-active-run limit. PSGC Silver and PHIVOLCS tests were cancelled before execution to reduce capacity pressure; they are not verified. Full Gold/Platinum execution remains unverified until the reviewed CMCI mapping is supplied; partial task successes do not certify the complete pipeline.

## Cost and performance

The jobs use Serverless environments and no recurring schedule. Task-level retries remain bounded; the parent does not automatically retry a whole failed ingestion job. Source extraction and spatial-join duration should be measured before increasing source coverage. The spatial boundary/event predicate is distributed and can be the most expensive task; keep the source window bounded to the approved rolling five years. Existing runtimes and dependencies are reused; no separate service or external data-quality package is introduced.

## Dashboard handoff

See the [dashboard operating guide](risk-preparedness-dashboard.md) for the four-question dashboard asset, query coverage, and the current SQL compute eligibility blocker. Existing committed merge markers in the runner and bundle were resolved using the Serverless dispatch pattern with the source-file inspection fallback retained. A complete dev rebuild is required after the `Mun` scope correction; earlier partial task success and empty metric snapshots do not establish analytical readiness.
