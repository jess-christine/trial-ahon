# CMCI

- **Source:** Department of Trade and Industry, Cities and Municipalities Competitiveness Index Data Portal
- **Coverage:** Philippine cities and municipalities, 2014 to 2024; loaded on demand from the public portal
- **Owner:** Project AHON data team

Source portal: [CMCI Data Portal](https://cmci.dti.gov.ph/data-portal.php)

## Scope

| Measure | Count |
| --- | ---: |
| PSGC cities and municipalities | 1,642 |
| Live CMCI locality values | 1,634 |
| Approved CMCI mappings | 1,634 |
| Known PSGC-only source gaps | 8 |
| Years requested | 11 |
| Indicators requested | 35 |

CMCI complements hazard datasets by describing local competitiveness, capacity, infrastructure, resiliency, and innovation. CMCI does not measure hazard occurrence, intensity, frequency, or geographic exposure.

## Bronze: `ahon.bronze.cmci_raw_indicator_batch_html`

- **One row is:** one CMCI source request containing one or more LGUs, all configured years, and all configured indicators
- **Loaded by:** `src/sql/01_bronze_ingest/cmci_batch_ingest.py`
- **Row hash covers:** the raw request arrays, expected and returned counts, raw response HTML, and response hash; excludes the five provenance columns

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `batch_id` | string | Deterministic identifier for the CMCI request batch | Derived from PSGC codes, years, and indicator codes |
| `requested_psgc_codes` | array<string> | Canonical PSGC codes included in the request | Used to determine completed LGU ingestion scope |
| `requested_cmci_names` | array<string> | Exact CMCI locality values submitted to the portal | Order corresponds to `requested_psgc_codes` |
| `requested_years` | array<string> | CMCI years requested | Currently 2014 to 2024 |
| `requested_indicator_codes` | array<string> | CMCI indicator codes requested | Currently 35 indicators |
| `expected_lgu_count` | integer | Number of LGUs expected in the response | Usually 10; the final batch may be smaller |
| `expected_year_count` | integer | Number of years expected in the response | Currently 11 |
| `expected_indicator_count` | integer | Number of indicator tables expected | Currently 35 |
| `returned_value_count` | integer | Number of validated values parsed from the response | Expected LGUs × years × indicators |
| `response_html` | string | Complete raw HTML returned by CMCI | Retained for audit and reprocessing |
| `response_hash` | string | Stable SHA-256 hash of parsed source values | Used to detect changed responses |
| `ingestion_timestamp` | timestamp (UTC) | Original pipeline ingestion timestamp | Retained for compatibility and source ordering |
| `_source_name` | string | Dataset or feed that produced the row | Always `cmci_data_portal` |
| `_source_ref` | string | Sanitized CMCI endpoint used for the request | Must not contain keys, tokens, or credentials |
| `_ingested_at` | timestamp (UTC) | When the row was written to Bronze | Standard Bronze provenance timestamp |
| `_batch_id` | string | Ingestion run identifier | May match `batch_id` when one source request is one ingestion unit |
| `_row_hash` | string | SHA-256 of the raw source columns | Excludes all provenance columns |

## Bronze: `ahon.bronze.cmci_raw_indicator`

- **One row is:** one raw CMCI indicator value for one PSGC LGU and year within a source batch
- **Loaded by:** `src/sql/01_bronze_ingest/cmci_batch_ingest.py`
- **Row hash covers:** `batch_id`, `psgc_code`, `psgc_name`, `cmci_name`, `indicator_label`, `year`, `raw_value`, `response_hash`, and `ingestion_timestamp`; excludes the five provenance columns

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `batch_id` | string | Batch containing the source value | Links to `cmci_raw_indicator_batch_html` |
| `psgc_code` | string | Canonical PSGC LGU identifier | Approved through `ahon.reference.cmci_lgu_map` |
| `psgc_name` | string | Canonical PSGC LGU name | Retained for traceability |
| `cmci_name` | string | Exact CMCI locality name | Source identity submitted to CMCI |
| `indicator_label` | string | CMCI indicator represented by the row | Must belong to the configured 35 indicators |
| `year` | string | CMCI reporting year | Currently 2014 to 2024 |
| `raw_value` | string | Raw value returned by CMCI | Blank and `-` become `NULL` in Silver |
| `response_hash` | string | Hash of the validated CMCI response | Links the row to its source response |
| `ingestion_timestamp` | timestamp (UTC) | Original source ingestion timestamp | Retained for compatibility and latest-record selection |
| `_source_name` | string | Dataset or feed that produced the row | Always `cmci_data_portal` |
| `_source_ref` | string | Sanitized CMCI endpoint used for the request | Must not contain keys, tokens, or credentials |
| `_ingested_at` | timestamp (UTC) | When the row was written to Bronze | Standard Bronze provenance timestamp |
| `_batch_id` | string | Ingestion run identifier | Links related rows from the same load run |
| `_row_hash` | string | SHA-256 of the raw source columns | Excludes all provenance columns |

## Reference: `ahon.reference.cmci_lgu_map`

- **One row is:** one PSGC LGU and its reviewed CMCI mapping status
- **Key:** `psgc_code`
- **Maintained by:** `src/sql/01_bronze_ingest/cmci_mapping_update.py`
- **Profiled by:** `src/sql/01_bronze_ingest/cmci_source_profile.py`

Only mappings meeting all three conditions are eligible for ingestion:

```text
match_status = MATCHED
reviewed = true
is_active = true
```

Approved mapping methods:

| Method | Count |
| --- | ---: |
| `EXACT` | 1,113 |
| `NORMALIZED` | 136 |
| `COMPACT` | 7 |
| `MANUAL` | 7 |
| `PROVINCE_SUFFIX` | 347 |
| `PROVINCE_SUFFIX_EXCEPTION` | 4 |
| `MANUAL_ALIAS` | 20 |
| **Total** | **1,634** |

## Silver Tables

The CMCI Silver layer contains one table per pillar:

```text
ahon.silver.cmci_economic_dynamism
ahon.silver.cmci_government_efficiency
ahon.silver.cmci_infrastructure
ahon.silver.cmci_resiliency
ahon.silver.cmci_innovation
ahon.silver.cmci_ingestion_batch_clean
```

The batch audit record is also summarized in `ahon.silver.cmci_ingestion_batch_clean` by `src/sql/02_silver_clean/cmci_batch_status.sql`. The raw HTML remains only in Bronze; Silver retains request scope, count metadata, response hash, provenance, and an `is_complete` flag based on the documented expected-count formula.

- **One row is:** one approved PSGC LGU and CMCI year for one pillar
- **Key:** `psgc_code + year`
- **Built from Bronze by:** `src/sql/02_silver_clean/cmci_indicator_parse.py`

### Common Silver Columns

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `psgc_code` | string | Canonical PSGC LGU identifier | Part of the logical key |
| `lgu` | string | Canonical PSGC locality name | Derived from the approved mapping table |
| `cmci_name` | string | Exact CMCI source locality name | Preserved for source traceability |
| `year` | string | CMCI reporting year | Part of the logical key |
| Pillar indicator columns | numeric | Typed CMCI indicator values | Blank or unavailable source values remain `NULL` |
| `source_response_hash` | string | Hash of the source response | Used to update changed source data |
| `source_ingestion_timestamp` | timestamp (UTC) | Timestamp of the selected Bronze source record | Latest valid source record wins |
| `silver_processed_timestamp` | timestamp (UTC) | Time the Silver row was processed | Set by the Silver parser |

### Expected Final Coverage

```text
1,634 approved LGUs × 11 years
= 17,974 LGU-year rows per Silver pillar table
```

Each Silver table must have:

```text
Rows: 17,974
Unique PSGC-year keys: 17,974
LGUs: 1,634
Years: 11
Duplicate PSGC-year keys: 0
Unauthorized PSGC codes: 0
```

## Known Issues

- Eight valid PSGC municipalities do not have separate locality values in the current CMCI portal:

  ```text
  1999901000 | Kapalawan
  1999902000 | Old Kaabakan
  1999903000 | Kadayangan
  1999904000 | Nabalawag
  1999905000 | Pahamuddin
  1999906000 | Malidegao
  1999907000 | Ligawasan
  1999908000 | Tugunan
  ```

- These eight LGUs remain unmatched and must not inherit values from former parent municipalities.
- Missing CMCI values are represented as `NULL`, not zero.
- Some indicators may be unavailable for particular LGUs or years.
- The CMCI portal is a public website rather than a documented high-volume API.
- The existing Delta tables require a one-time schema migration before writers populate the five standard Bronze provenance columns.
- A reproducible baseline CMCI mapping bootstrap still requires validation for fresh-environment deployment.

## Quality and open items

- Run Bronze checks with `src/sql/monitoring/bronze_quality.py`; outcomes are appended to `ahon.monitoring.dq_result`. The checks validate standard provenance and SHA-256 format.
- `src/sql/02_silver_clean/cmci_indicator_parse.py` selects the most recent complete request batch for each `psgc_code + year`, pivots the configured indicators into the five documented pillar tables, preserves missing values as `NULL`, and merges on the documented key. It does not synthesize missing LGU-year rows.
- Duplicate LGU-year-indicator keys in the selected batch stop the parser so an arbitrary aggregate cannot hide conflicting values.
- `src/sql/02_silver_clean/cmci_batch_status.sql` maps the raw HTML audit table to a compact Silver batch status table. Raw HTML remains in Bronze.
- Full expected coverage is 17,974 rows per pillar after a full ingestion. Test-mode or partial Bronze batches naturally produce less and are not treated as complete coverage; no Databricks full-run has verified that count.
- Existing Delta tables deployed before the provenance schema change may need a controlled schema migration before the validator can run.
- A reproducible baseline CMCI mapping bootstrap still requires validation for fresh-environment deployment.

## Related Documentation

- [PSGC Dataset](psgc.md)
- [CMCI Mapping and Ingestion Decision](../decisions/0001-cmci-mapping-and-ingestion.md)
- [Data Ingestion](ingestion.md)
- [Source Profiles](source-profiles.md)
- [Data Validation](validation.md)
- [Operations Runbook](../operations/runbook.md)
