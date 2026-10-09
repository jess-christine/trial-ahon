# dim_lgu

- **Purpose:** canonical current LGU dimension for the earthquake preparedness model.
- **Source and lineage:** active rows from `ahon.reference.lgu_master`; hierarchy codes are enriched by exact `psgc_code` matches in versioned `ahon.silver.psgc_clean`. The reviewed reference supplies the key, name, level, and province code. No name matching is used.
- **One row is:** one active city or municipality in the reviewed reference (currently 1,642 rows when the documented reference snapshot is loaded).
- **Key:** `psgc_code`.
- **Built by:** `src/sql/gold/build_gold.py`; table created by `src/sql/00_setup/07_gold_setup.sql`.
- **Business questions:** establishes canonical LGU identity for CMCI and future risk/preparedness facts.

| Column | Type | Description | Notes |
|---|---|---|---|
| `psgc_code` | varchar(10) | Official LGU code | Primary key; comes from reviewed reference |
| `area_name` | varchar(255) | Current LGU name | Comes from reviewed reference |
| `geographic_level` | string | PSGC geographic classification | Current reference covers cities and municipalities |
| `region_code` | varchar(10) | Parent region code | From PSGC Silver exact-code match; `NULL` if unavailable or versions conflict |
| `province_code` | varchar(10) | Parent province code | Reviewed reference wins; falls back to PSGC Silver when reference value is absent |
| `municipality_code` | varchar(10) | Parent municipality code | From PSGC Silver exact-code match; `NULL` if unavailable or versions conflict |
| `barangay_code` | varchar(10) | Barangay code | Current reference grain has no barangay rows; generally `NULL` |

## Quality and operations

The build requires unique, active reference codes. It checks hierarchy consistency across all available PSGC versions; when versions disagree, the dimension row is retained with the affected hierarchy codes set to `NULL`. Missing and conflicting hierarchy values and province disagreements are warnings recorded by `silver_gold_quality.py`. Duplicate keys and required reference fields block the refresh. Reruns replace the dimension snapshot atomically after schema checks.

This dimension intentionally follows the owner's selected active `lgu_master` scope. It does not include province, region, or barangay dimension rows. A future expansion requires a new canonical source/coverage decision. Gold facts join only on the exact PSGC key.
