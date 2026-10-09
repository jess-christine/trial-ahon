# dim_cmci_indicator

- **Purpose:** approved CMCI indicator lookup for the CMCI fact.
- **Source and lineage:** `INDICATORS_BY_PILLAR` in `src/sql/01_bronze_ingest/cmci_common.py`, the shared approved indicator configuration used by CMCI ingestion and Silver parsing.
- **One row is:** one configured indicator code.
- **Key:** `indicator_code`.
- **Built by:** `src/sql/gold/build_gold.py`; table created by `src/sql/00_setup/07_gold_setup.sql`.
- **Business questions:** supports experimental Platinum v1 preparedness analysis and identification of comparatively low CMCI indicators among the highest-priority LGUs.

| Column | Type | Description | Notes |
|---|---|---|---|
| `indicator_code` | varchar(10) | Stable CMCI indicator code | Primary key; 35 codes from shared configuration |
| `pillar_name` | varchar(50) | CMCI pillar | One of the five configured pillars |
| `indicator_name` | varchar(255) | CMCI indicator label | Exact approved config label |

## Quality and operations

The build rejects duplicate or out-of-contract indicator codes and replaces this small configuration snapshot deterministically. Layer quality checks require all fields, enforce unique codes, and reconcile the row count with the approved configuration. The table is independent of the CMCI batch history and can be rebuilt at low cost.
