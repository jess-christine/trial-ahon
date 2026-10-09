# preparedness_gap_by_lgu

- **Purpose:** compare experimental preparedness and risk for “How far does preparedness fall short of risk in each LGU?”
- **Source:** `risk_level_by_lgu`, `fact_cmci_indicator`, `dim_cmci_indicator`, and `fact_ldrrmf`.
- **One row is:** one active city/municipality per calculation run.
- **Key:** `psgc_code` within `run_id`.
- **Built by:** `src/sql/platinum/build_platinum.py`.
- **Run:** DAB `build_platinum` task after Gold build and validation.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `psgc_code` | string | Official active city/municipality code | |
| `risk_score` | decimal(12,2) | Experimental risk score | 0–10,000 |
| `risk_tertile` | int | Risk cohort | 1 low to 3 high |
| `cmci_year` | int | Latest year with CMCI data for this LGU | |
| `cmci_capacity_score` | decimal(7,2) | Mean within-indicator CMCI percentile | 0–100; requires all indicators |
| `fiscal_year` | int | Latest matched LDRRMF year | |
| `ldrrmf_utilization_score` | decimal(7,2) | Total expenditure / appropriation × 100 | Null when denominator is zero or unavailable |
| `preparedness_score` | decimal(8,2) | CMCI capacity plus utilization | 0–200; null unless both inputs exist |
| `preparedness_gap` | decimal(14,2) | Preparedness score minus risk score | Ranked descending by owner-approved instruction |
| `preparedness_tertile` | int | Preparedness cohort | 1 low to 3 high |
| `preparedness_gap_rank` | int | Descending gap rank | |
| `run_id` | string | Calculation run identifier | |
| `calculated_at` | timestamp | Calculation time in UTC | |

Completeness of CMCI indicators is required; unmatched LDRRMF does not contribute. The risk and preparedness scales differ, so the gap is not a calibrated deficit. Exact component, normalization, and missing-value assumptions are in [experimental metric assumptions](platinum_analytics.md).
