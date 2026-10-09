# 0007: Experimental city/municipality Platinum v1

- **Status:** Accepted for experimental v1
- **Date:** 2026-10-09

## Context

The project needed runnable risk, preparedness, vulnerability, and CMCI priority outputs, but source/metric choices were previously open. The owner approved an explicitly experimental percentile/tertile approach and approximate Geoportal municipality boundaries. PSA has city/municipality, not barangay, population.

## Decision

Use latest matched PSA city/municipality total-population percentile (0–100); combine equal-weight percentiles of event count, mean magnitude, and inverse mean distance from event to approximate LGU centroid for a rolling five-year window; multiply the two 0–100 scores for risk; use cross-sectional tertiles for risk and vulnerability categories. CMCI capacity is the mean percentile across the full approved indicator set. LDRRMF utilization is total expenditure / total appropriation × 100, null for zero/missing appropriation. Missing metric components yield null scores. Use the supplied descending preparedness-gap ranking. Match BLGF to active LGU reference by exact LGU + province names only; ambiguous/unmatched remain null. Match PHIVOLCS by a unique approximate Geoportal polygon only.

## Why

- Uses source attributes and table grains already represented in the repository.
- Makes assumptions explicit and repeatable while avoiding fabricated matches, allocations, thresholds, or missing values.
- Approximate polygons are the only approved boundary source for this prototype; the spatial limitation remains visible.

## Alternatives considered

- **Leave Platinum unimplemented:** would not meet the owner's immediate request for calculated metrics.
- **Infer missing events or name matches:** rejected because it would hide source coverage and change the meaning without a reviewed rule.
- **Use precise operational cutoffs:** no calibration data or approved thresholds are available.

## Consequences

Four Platinum snapshots are built on each successful run and include run identifiers and calculation timestamps. Unmatched source rows stay visible. Risk and preparedness scales differ (up to 10,000 vs 200); the gap is an experimental ordering aid, not a calibrated deficit. Zero-event areas have missing magnitude/proximity and null risk. The approved GeoPortal boundary artifact and its PSGC property must be configured in Databricks. Production use requires calibration and owner review.

## Open

GeoPortal boundary file access and its exact PSGC property name must be supplied/configured. PHIVOLCS source timestamps do not include a timezone; the current jobs use UTC Spark sessions. Confirm whether source wall time is UTC or Asia/Manila before interpreting the five-year cutoff. Validate observed coverage, match ambiguity, event window, and metric distributions in Databricks before communicating outputs.
