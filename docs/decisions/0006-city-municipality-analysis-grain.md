# 0006: City/municipality analysis grain

- **Status:** Accepted
- **Date:** 2026-10-09

## Context

The initial target diagram described `fact_population` at barangay grain, but the available PSA 2024 source contains national, region, province, and city/municipality records only. The project owner clarified that all analytical items should use cities and municipalities and that the barangay population fact should be disregarded. The active reviewed `lgu_master` is the canonical city/municipality dimension source.

## Decision

- Build `fact_population` from only PSA city/municipality rows, using the PSA `geographic_location` and year as its key.
- Match a PSA row to active `lgu_master` only by exact `lgu_name` and `province_name`. Populate `psgc_code` only for one unique candidate; retain unmatched and ambiguous population rows with null `psgc_code`.
- Build the Gold LDRRMF fact from City and Municipality rows only. Province and any unsupported-type rows remain in Bronze/Silver and are counted as explicit Gold scope exclusions.
- Keep population metrics unnormalized and keep all other source values at their documented source grain until a separately approved mapping exists.

## Why

This aligns the analytical grain with the available PSA source and the active reviewed LGU reference without distributing municipal population to barangays or guessing PSGC matches.

## Consequences

- The Gold population fact schema is city/municipality path-year, not barangay-year; consumers must use the nullable PSGC key for dimension joins and retain unmatched rows for coverage reporting.
- Exact text differences and missing or ambiguous province pairs reduce matched coverage; the quality result reports null matches.
- Risk and preparedness metrics remain blocked on their unresolved normalization, component, threshold, missing-value, and ranking definitions.
- Bronze and Silver preserve full source coverage, including PSA hierarchy rows outside the selected Gold grain and BLGF province reports.
