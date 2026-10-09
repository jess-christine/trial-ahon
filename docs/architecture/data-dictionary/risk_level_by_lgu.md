# risk_level_by_lgu

- **Purpose:** summarize relative earthquake risk for the question “What is the earthquake risk level of each LGU?”
- **Source:** Gold population and event facts, active `dim_lgu`, and approximate Silver Geoportal boundaries.
- **One row is:** one active city/municipality per calculation run.
- **Key:** `psgc_code` within `run_id`.
- **Built by:** `src/sql/platinum/build_platinum.py`.
- **Run:** `build_platinum` task in the Databricks Asset Bundle after boundary load, Gold build, and Silver/Gold DQ.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `psgc_code` | string | Official active city/municipality code | |
| `population_year` | int | Latest matched PSA population year | City/municipality only |
| `population_exposure_score` | decimal(7,2) | Within-year population percentile | 0–100 |
| `earthquake_activity_score` | decimal(7,2) | Mean event-count, magnitude, and inverse-distance percentiles | 0–100; rolling five years |
| `risk_score` | decimal(12,2) | Population exposure times earthquake activity | 0–10,000 |
| `risk_tertile` | int | Cross-sectional risk tertile | 1 low to 3 high |
| `risk_level` | string | Tertile label | Low, Medium, High |
| `risk_window_start` | date | Start of the rolling activity window | |
| `risk_window_end` | date | End of the rolling activity window | |
| `run_id` | string | Calculation run identifier | |
| `calculated_at` | timestamp | Calculation time in UTC | |

Missing population or any unavailable event component produces null risk. No-event LGUs have no mean magnitude/proximity and remain null. A unique spatial match uses approximate Geoportal polygons; unmatched and boundary-overlap events do not contribute. See [experimental metric assumptions](platinum_analytics.md). The score is a cross-sectional prototype, not a calibrated probability or validated operational risk category.
