# psgc

- **Source:** PSA Philippine Standard Geographic Code API, queried by publication period from the bundle-configured API base URL.
- **Coverage:** records returned by the bundle-configured PSGC periods; several versions may contain the same code.
- **Owner:** Project AHON data team.
- **Business purpose:** provide canonical geographic codes and hierarchy attributes used to link source datasets to LGUs.

## Bronze: `ahon.bronze.psgc`

- **One row is:** one PSGC API record for one code and source version.
- **Key:** source `psgc_code + version`; the Bronze loader merges by `_row_hash` to retain source revisions.
- **Loaded by:** `src/sql/datasets/psgc/psgc_api.py`; API base URL and publication periods are job parameters, and credentials come from Databricks Secrets.
- **Row hash covers:** the 16 source fields in the loader's `raw_cols`, before provenance is added.

The API's nested `populations` field is serialized as JSON text.

| Column | Type | Description | Notes |
|---|---|---|---|
| `psgc_code` | string | PSGC code for the record | Source `code` |
| `area_name` | string | Geographic name | Source `area_name` |
| `correspondence_code` | string | Source correspondence code | |
| `geographic_level` | string | Geographic classification | |
| `region_code` | string | Parent region code | Source `reg` |
| `province_code` | string | Parent province code | Source `prv` |
| `municipality_code` | string | Parent municipality code | Source `mun` |
| `barangay_code` | string | Barangay code | Source `bgy` |
| `old_name` | string | Historical name | |
| `city_class` | string | City classification | |
| `income_classification` | string | Income classification | Retained as a source attribute, not used to calculate risk |
| `urban_rural` | string | Urban/rural classification | |
| `island_region` | string | Island group or region label | |
| `status` | string | Source record status | |
| `version` | string | Source publication period | |
| `populations_json` | string | JSON text for nested source population values | |
| `_source_name` | string | Bronze dataset name | `psgc` |
| `_source_ref` | string | API base URL | Token is excluded |
| `_ingested_at` | timestamp (UTC) | Bronze load time | |
| `_batch_id` | string | Load run identifier | |
| `_row_hash` | string | SHA-256 of the raw source fields | Excludes provenance |

## Silver: `ahon.silver.psgc_clean`

- **One row is:** one PSGC code and source version with trimmed identifiers and labels.
- **Key:** `psgc_code + version`.
- **Built from Bronze by:** `src/sql/datasets/psgc/silver/psgc_clean.sql`.

Silver trims identifier and label fields, converts blank optional codes and labels to `NULL`, retains all source versions and carries Bronze provenance forward. It does not filter to active records or replace the existing `ahon.reference.lgu_master`; the reference loader and its source remain governed by [docs/data/psgc.md](../../data/psgc.md). Geography can support the established `dim_lgu` attributes, but this staging transform does not create or assign downstream fact matches.

| Column | Type | Description | Notes |
|---|---|---|---|
| `psgc_code` | string | PSGC code | Key part; trimmed from Bronze |
| `area_name` | string | Geographic name | |
| `geographic_level` | string | Geographic classification | |
| `region_code` | string | Parent region code | |
| `province_code` | string | Parent province code | |
| `municipality_code` | string | Parent municipality code | |
| `barangay_code` | string | Barangay code | |
| `correspondence_code` | string | Source correspondence code | |
| `old_name` | string | Historical name | |
| `city_class` | string | City classification | |
| `income_classification` | string | Income classification | |
| `urban_rural` | string | Urban/rural classification | |
| `island_region` | string | Island group or region label | |
| `status` | string | Source record status | |
| `version` | string | Source publication period | Key part |
| `populations_json` | string | JSON text for nested population values | |
| `_source_name` | string | Source system | Carried from Bronze |
| `_source_ref` | string | Source API reference | Carried from Bronze |
| `_ingested_at` | timestamp (UTC) | Bronze load time | Carried from Bronze |
| `_batch_id` | string | Bronze load run identifier | Carried from Bronze |
| `_row_hash` | string | Bronze raw-row hash | Carried from Bronze |

## Quality and limitations

`src/sql/monitoring/bronze_quality.py` reports code format, required geography, duplicate code-version keys, and provenance checks to `ahon.monitoring.dq_result`. These source anomalies are warnings and do not alter Bronze. API coverage and active-status semantics are source-version dependent; consumers should use the reviewed reference table for canonical LGU joins.

## Silver hierarchy key correction

Bronze `reg`, `prv`, `mun`, and `bgy` values are source numeric components,
not full 10-digit dimension keys, and remain unchanged. Silver resolves full
region/province/municipality keys against explicit Reg/Prov/City/Mun records in
the same source version, using the corresponding 2/5/7-digit PSGC prefix.
Missing parent records produce null keys. Only Bgy rows populate `barangay_code`.
Parent lookups use distinct code/version/level tuples to preserve source row count;
source duplicates remain observable. Gold still uses active reviewed `lgu_master`
and gives its reviewed province code precedence; historical hierarchy conflicts
remain warnings. See the [PSA PSGC reference](https://psa.gov.ph/classification/psgc).

The observed Q2_2021 code `133900000` identifies both Manila City and its
statistical district. Silver preserves both source records; `unique_code_version`
is a warning consistent with Bronze source-anomaly policy. This historical
9-digit collision cannot supply an exact key to the reviewed 10-digit LGU
reference. Canonical Gold key uniqueness remains blocking.
