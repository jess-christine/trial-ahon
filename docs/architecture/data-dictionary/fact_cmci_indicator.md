# fact_cmci_indicator

- **Purpose:** preserve comparable source indicator values at the approved analytical grain.
- **Source and lineage:** the five `ahon.silver.cmci_*` pillar tables, built from `ahon.bronze.cmci_raw_indicator` and filtered to complete CMCI request batches. `psgc_code` is resolved through the reviewed CMCI map and joined to `dim_lgu`; `indicator_code` joins to `dim_cmci_indicator`.
- **One row is:** one PSGC LGU, CMCI year, and indicator code.
- **Key:** (`psgc_code`, `year`, `indicator_code`).
- **Built by:** `src/sql/gold/build_gold.py`; schema in `src/sql/00_setup/07_gold_setup.sql`.
- **Business questions:** supplies observed CMCI capacity/resiliency indicators for later preparedness and priority analysis. It does not define a composite preparedness score.

| Column | Type | Description | Notes |
|---|---|---|---|
| `psgc_code` | varchar(10) | Canonical PSGC LGU code | Foreign key to `dim_lgu` |
| `year` | int | CMCI reporting year | Part of the fact key |
| `indicator_code` | varchar(10) | CMCI indicator | Foreign key to `dim_cmci_indicator` |
| `psgc_name` | varchar(255) | PSGC name carried by CMCI Silver | Source name remains visible |
| `cmci_name` | varchar(255) | Exact locality name used by CMCI | Preserved for traceability |
| `geographic_level` | varchar(50) | Current LGU level | From `dim_lgu` |
| `raw_score` | decimal(18,6) | Typed CMCI source value | Not normalized or rescaled; unavailable values remain `NULL` |

## Quality and operations

The build unpivots the configured Silver columns without aggregating indicators. It blocks missing LGU dimension or indicator references, null keys, duplicate fact keys, and source-to-fact count mismatches. Missing scores warn and remain `NULL`. Layer validation records Silver table coverage, missing score counts by indicator, fact key uniqueness, both dimension references, and the sum-of-pillar-rows-to-fact-row reconciliation.

The table is a full snapshot of available Silver history and is safe to rebuild. The Gold schema does not add provenance columns; source response hashes and ingestion timestamps remain in Silver and can be joined by PSGC/year/indicator-to-pillar lineage when needed. CMCI still has eight known PSGC-only LGUs without separate source values; no zeros are synthesized.
