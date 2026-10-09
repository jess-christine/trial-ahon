# Platinum analytical outputs (experimental v1)

- **Purpose:** answer the project risk, preparedness-gap, vulnerability, and priority questions at city/municipality level.
- **Source and lineage:** Gold `dim_lgu`, `fact_population`, `fact_earthquake_event`, `fact_cmci_indicator`, `dim_cmci_indicator`, and `fact_ldrrmf`; event LGU assignment uses Silver `lgu_boundary_clean` through Gold match fields.
- **Built by:** `src/sql/platinum/build_platinum.py` after `src/sql/gold/build_gold.py` and Silver/Gold validation.
- **Run:** Databricks Asset Bundle job `ahon_end_to_end`; see [bundle operations](../../operations/databricks-bundle.md).
- **Audit:** each output row has `run_id` and `calculated_at`; coverage and missing component counts are appended to `ahon.monitoring.dq_result`.

The owner approved the following assumptions for experimental v1. They are not calibrated operational thresholds:

1. Population exposure is the within-latest-census-year percentile of matched city/municipality total population, 0–100.
2. Earthquake activity uses a rolling five-year window and the equal-weight mean of percentiles for event count, mean magnitude, and inverse mean event-to-LGU-centroid distance. Distances use geodesic meters. Event assignments use approximate Geoportal polygons.
3. `risk_score = population_exposure_score * earthquake_activity_score`, from 0 to 10,000. Risk levels use cross-sectional tertiles.
4. CMCI capacity is the average percentile of all available CMCI indicators, scored within indicator and year; an LGU with incomplete indicator coverage has null capacity.
5. LDRRMF utilization is total expenditure / total appropriation * 100; zero/missing appropriation leaves the rate null.
6. `preparedness_score = cmci_capacity_score + ldrrmf_utilization_score`; if either input is null, preparedness and gap are null.
7. `preparedness_gap = preparedness_score - risk_score`; output ranks this descending as approved. The two components use different ranges (0–200 vs 0–10,000), so this gap is not a comparable-scale deficit.
8. Vulnerability labels use high-risk tertile plus low-preparedness tertile. CMCI priorities rank the lowest mean indicator percentiles among those highest-priority LGUs.

## `risk_level_by_lgu`

- **Grain/key:** one active city/municipality `psgc_code` per run.
- **Columns:** `psgc_code`, `population_year`, `population_exposure_score`, `earthquake_activity_score`, `risk_score`, `risk_tertile`, `risk_level`, `risk_window_start`, `risk_window_end`, `run_id`, `calculated_at`.
- **Null behavior:** no matched population or missing earthquake components yields null risk. No events means magnitude and proximity are unavailable, so activity remains null; it is not silently treated as zero.

## `preparedness_gap_by_lgu`

- **Grain/key:** one active city/municipality per run.
- **Columns:** `psgc_code`, `risk_score`, `risk_tertile`, `cmci_year`, `cmci_capacity_score`, `fiscal_year`, `ldrrmf_utilization_score`, `preparedness_score`, `preparedness_gap`, `preparedness_tertile`, `preparedness_gap_rank`, `run_id`, `calculated_at`.
- **Null behavior:** incomplete CMCI coverage, unmatched LDRRMF, missing amounts, or zero appropriation result in null component/preparedness/gap as applicable.

## `vulnerability_priority_by_lgu`

- **Grain/key:** one active city/municipality per run; carries the preparedness and risk attributes above plus `priority_category` (`Highest priority`, `Monitor`, `Capacity concern`, or `Lower priority`). Ineligible/missing tertiles remain null.

## `cmci_preparedness_priority_by_indicator`

- **Grain/key:** CMCI year and indicator code; describes the mean relative indicator score among highest-priority LGUs, count of contributing LGUs, and ascending `priority_rank`.
- **Columns:** `cmci_year`, `indicator_code`, `pillar_name`, `indicator_name`, `mean_indicator_percentile`, `priority_lgu_count`, `priority_rank`, `run_id`, `calculated_at`.
- **Empty output:** no highest-priority LGUs produces an empty snapshot; it does not fabricate a priority cohort.

## Quality and limitations

Coverage rules warn for active LGUs lacking population, spatially matched events, or complete preparedness inputs. Source anomalies and unmatched rows remain in Bronze/Silver/Gold. The spatial join requires a valid boundary file, unique PSGC polygon membership, and Runtime 17.1+; approximate boundaries can assign points incorrectly. Only one PHIVOLCS event per polygon is counted once; points covered by multiple polygons are excluded from event aggregation and remain ambiguous in Gold. Current outputs overwrite deterministic table snapshots; a failed run leaves prior snapshots available until a write succeeds. DQ history is append-only.

The cross-sectional percentiles and tertiles depend on each run's observed cohort. The 2024 PSA extract and available CMCI/LDRRMF reporting years constrain temporal comparability. No infrastructure exposure, population density, or income-class term is included in these formulas. Interpret all results as experimental until the owner calibrates and approves thresholds, units, and production use.
