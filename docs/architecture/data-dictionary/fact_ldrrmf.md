# fact_ldrrmf

- **Purpose:** retain source LDRRMF appropriations and expenditures for preparedness analysis.
- **Source and lineage:** `ahon.silver.blgf_ldrrmf_annual_lgu_clean`, from the seven annual BLGF workbooks in Bronze.
- **One row is:** one original BLGF city or municipality report line for one fiscal year, containing both 70% and 30% fund measures. Province lines remain in Bronze and Silver and are excluded from this city/municipality Gold fact.
- **Key:** `ldrrmf_fact_key`, a deterministic 64-bit hash of the Bronze source reference and row hash. Duplicate source rows that collide on this key block the Gold write; no row is silently removed.
- **Built by:** `src/sql/gold/build_gold.py`; schema in `src/sql/00_setup/07_gold_setup.sql`.
- **Business questions:** provides observed budget and expenditure inputs for later preparedness analysis.

| Column | Type | Description | Notes |
|---|---|---|---|
| `ldrrmf_fact_key` | bigint | Deterministic warehouse key | Derived from source lineage; collisions are blocking |
| `psgc_code` | varchar(10) | Matched PSGC code | `NULL` until an LGU matching rule is reviewed |
| `fiscal_year` | int | Fiscal reporting year | Parsed from source filename in Silver |
| `region` | varchar(255) | Source region | Preserved from BLGF |
| `province` | varchar(255) | Source province | Preserved from BLGF |
| `lgu_name` | varchar(255) | Source LGU name | Preserved from BLGF |
| `lgu_type` | varchar(50) | Source LGU type | Preserved from BLGF |
| `appropriation_70_pct` | decimal(18,2) | 70% appropriation | Pesos |
| `expenditure_70_pct` | decimal(18,2) | 70% expenditure | Pesos |
| `appropriation_30_pct` | decimal(18,2) | 30% appropriation | Pesos |
| `expenditure_30_pct` | decimal(18,2) | 30% expenditure | Pesos |
| `total_appropriation` | decimal(18,2) | Total appropriation | Pesos |
| `total_expenditure` | decimal(18,2) | Total expenditure | Pesos |
| `utilization_rate` | decimal(7,2) | Utilization rate | `NULL` until the project owner approves the formula and zero-appropriation behavior |
| `match_status` | psgc_match_status | PSGC matching state | Currently `UNMATCHED`; physical Databricks type is `STRING` |
| `match_confidence` | decimal(5,4) | Matching confidence | `NULL` because no approved match was attempted |

## Quality and operations

City and municipality Silver rows flow through in a deterministic snapshot. Silver-to-Gold row counts for those types and generated-key uniqueness are blocking checks; excluded province counts are written as a warning with their Silver retention location. Unmatched geography and unresolved utilization are warnings, preserving the data without implying a preparedness score. This fact does not derive utilization or apply name matching. Existing Bronze sum/amount checks remain warnings where the source findings allow legitimate exceptions.
