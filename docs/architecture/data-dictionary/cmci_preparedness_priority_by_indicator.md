# cmci_preparedness_priority_by_indicator

- **Purpose:** rank preparedness indicators to prioritize for the highest-priority LGUs.
- **Source:** `fact_cmci_indicator`, `dim_cmci_indicator`, and `vulnerability_priority_by_lgu`.
- **One row is:** one CMCI indicator and year, aggregated across LGUs labeled Highest priority.
- **Key:** `cmci_year`, `indicator_code` within `run_id`.
- **Built by:** `src/sql/platinum/build_platinum.py`.
- **Run:** DAB `build_platinum` task after risk, preparedness, and vulnerability outputs.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `cmci_year` | int | CMCI reporting year | |
| `indicator_code` | string | Approved CMCI indicator code | |
| `pillar_name` | string | Indicator pillar | From `dim_cmci_indicator` |
| `indicator_name` | string | Indicator name | From `dim_cmci_indicator` |
| `mean_indicator_percentile` | decimal(7,2) | Mean relative score among highest-priority LGUs | Lower values rank as more urgent |
| `priority_lgu_count` | long | Number of contributing priority LGUs | |
| `priority_rank` | int | Ascending mean-percentile rank | |
| `run_id` | string | Calculation run identifier | |
| `calculated_at` | timestamp | Calculation time in UTC | |

Only the latest available CMCI year per LGU is used. An empty result means no highest-priority LGUs had usable CMCI indicators; the pipeline does not manufacture a cohort. Indicator scores are percentiles within indicator/year; see [experimental metric assumptions](platinum_analytics.md).
