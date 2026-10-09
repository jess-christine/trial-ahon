# Databricks Asset Bundle

## Scope and run order

The `ahon_end_to_end` job in `resources/ahon_pipeline.yml` runs setup, reference load, source extraction/Bronze, Bronze DQ, Silver transforms, boundary load, Gold build, Silver/Gold DQ, and experimental Platinum in dependency order. The job has one shared small job cluster, one concurrent run, and one retry. It is manual; no schedule was added.

## Configure and deploy

Use the Databricks CLI version supported by the workspace. Authenticate with an existing profile and run commands from the repository root:

```powershell
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev ahon_end_to_end
```

Use `-t prod` only after the development output and mappings are reviewed. The bundle does not include source files or credentials. Configure `node_type_id`, `psgc_secret_scope`, `psgc_secret_key`, and `boundary_code_property` for the workspace. Catalog and volume paths, source endpoints, PSA census year, PSGC periods, CMCI reporting years, and the PHIVOLCS lookback are bundle variables. The job uses DBR 17.1+ because native geospatial SQL functions are required.

Before running, ensure:

- The active PSGC publication workbook is landed at the reference source volume root for `load_lgu_master.py`.
- The source-volume folders/files needed by PSA/BLGF/PHIVOLCS are accessible; extractor tasks land source files where their Bronze loaders expect them.
- A reviewed active CMCI locality mapping is present in `reference.cmci_lgu_map`. The job does not create or approve mappings; CMCI fails if reviewed coverage is insufficient.
- The owner-approved approximate Geoportal city/municipality GeoJSON FeatureCollection is landed at the configured path. Set `boundary_code_property` to the source's actual property containing official PSGC codes; the loader does not infer code fields.
- The deployment identity can create/use the catalog, schemas, volume, tables, and job cluster, and can read the configured secret.

The boundary inventory currently reports that downloads are temporarily unavailable. The code is ready for the approved GeoJSON file, but without it boundary load blocks Gold/Platinum execution. GeoPortal polygons are approximate, and that limitation carries into spatial matches and risk outputs.

## Configuration and reliability

`AHON_CATALOG`, `AHON_SOURCE_VOLUME`, source endpoints, the PSA census year, CMCI reporting years, and PHIVOLCS lookback come from bundle variables. `AHON_BOUNDARY_GEOJSON_PATH` and `AHON_BOUNDARY_CODE_PROPERTY` are job-cluster environment variables. PSGC credentials are resolved from a Databricks secret by scope/key identifiers; secret values are not placed in YAML. SQL scripts run through `src/sql/common/run_sql_file.py`, which validates catalog identifiers, substitutes the configured catalog and source-volume root, and splits statements outside quoted strings/comments. CMCI ingestion and its source profiler share one configured year list.

Bronze boundary ingestion merges on source reference and feature hash, retaining previous source versions while making identical reruns idempotent. Silver, Gold, and Platinum are deterministic snapshots; DQ results append run-scoped outcomes. Source extract tasks may call external endpoints and therefore rely on the repo's existing source-specific retries/validation. A failed upstream task prevents dependent layers from running.

## Monitoring and failure behavior

Bronze and Silver/Gold DQ write to `monitoring.dq_result`. Blocking schema, provenance, duplicate-key, malformed geometry, and referential-integrity failures fail their task after results are recorded. Documented source anomalies, coverage gaps, unmatched geography, and approximate-boundary coverage remain warnings. Platinum reports missing LGU coverage for population, events, and preparedness; null components are preserved and no row is silently filtered from the risk-level snapshot.

Review each task output and query the latest `run_id` in `monitoring.dq_result` before publishing results. The bundle has not been deployed or run against a live Databricks workspace in this development session.

## Cost and performance

The job uses a single-worker cluster by default and no recurring schedule. Source extract and spatial-join duration should be observed before increasing workers. The spatial boundary/event predicate is distributed and can be the most expensive task; keep the source window bounded to the approved rolling five years. Existing runtimes and dependencies are reused; no separate service or external data-quality package is introduced.
