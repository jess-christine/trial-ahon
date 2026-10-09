# fact_population

- **Purpose:** provide observed city/municipality population and urbanization measures for population exposure analysis.
- **Business question:** what population measure is available for each city or municipality?
- **Source and lineage:** `ahon.silver.psa_population_clean`, from `ahon.bronze.psa_population_raw`; exact `lgu_name` + `province_name` matching against active `ahon.reference.lgu_master` supplies `psgc_code` only when there is exactly one candidate.
- **One row is:** one PSA city/municipality `geographic_location` for one census year. Other source levels are not rolled into the fact.
- **Key:** (`geographic_location`, `year`), the documented PSA path-year natural key. `psgc_code` is nullable so an unmatched or ambiguous source row remains represented.
- **Built by:** `src/sql/gold/build_gold.py`; schema in `src/sql/00_setup/07_gold_setup.sql`.
- **Relationships:** a non-null `psgc_code` references `dim_lgu.psgc_code`; unmatched rows have no dimension relationship yet.
- **Business use:** source input for population exposure, not a normalized score. No synthetic barangay values are created.

| Column | Type | Description | Notes |
|---|---|---|---|
| `geographic_location` | string | Full PSA geographic path | Source natural key component; retained for traceability |
| `psgc_code` | varchar(10) | Exact reviewed LGU match | Nullable; matching is exact LGU name and province name, not fuzzy |
| `year` | int | Census year | Derived from the PSA Silver source |
| `total_population` | bigint | Total population | PSA value |
| `urban_population` | bigint | Population in urban areas | PSA value |
| `percent_urban` | double | Percent urban | PSA source scale, 0 to 100 |

## Quality and operations

The build filters Silver to `geographic_level = 'City/Municipality'`; it does not alter Bronze or Silver. The city/municipality Silver count must equal the Gold row count, and (`geographic_location`, `year`) must be unique. Missing or out-of-range measures are warnings, as are null PSGC matches; these rows are retained. Gold validation records matched/unmatched counts through rule outcomes and checks source-to-target row reconciliation.

Run `src/sql/00_setup/07_gold_setup.sql`, then `src/sql/gold/build_gold.py`, then `src/sql/monitoring/silver_gold_quality.py`. The builder replaces the deterministic Gold snapshot after validating the target schema. It scans the PSA city/municipality subset and the small active LGU reference; no driver-side source collection or additional dependency is used.

## Limitations and open decisions

PSA reports repeated place names across provinces, which is why both names are required for matching. Exact text differences and active LGUs without province assignment remain unmatched. The current data dictionary documents the 2024 extract only. Platinum uses the latest matched total-population value and a within-year percentile; this is experimental and must be interpreted with the population coverage and time-window limitation documented in [Platinum metrics](platinum_analytics.md).
