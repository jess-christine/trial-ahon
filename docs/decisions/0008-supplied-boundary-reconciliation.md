# 0008: Supplied boundary reconciliation

- **Status:** Accepted for experimental preparation under the owner's request to map the supplied artifact
- **Date:** 2026-10-09

## Context

The owner supplied a BetterGov-derived GeoParquet reference with mixed PH-prefixed and current PSGC identifiers. The existing loader expects GeoJSON with ten-digit PSGC keys.

## Decision

Prepare a GeoJSON landing representation and complete crosswalk using official PSGC publication codes and exact name corroboration; use globally unique exact names only when code reconciliation fails. Preserve every source field, original WKB, unresolved row, and mapping method/reference. Unmatched codes warn and remain null; malformed non-null codes and duplicate assigned keys block. Retain the original artifact. Do not infer CMCI mappings from this file.

## Why

Reuses the existing loader and schema without an additional recurring task, runtime framework, fuzzy matches, or invented geometry.

## Alternatives considered

Direct Parquet ingestion would require a second Bronze contract. Unverified code conversion alone can assign changed or incorrect identities; exact corroboration is required.

## Consequences

The source is curated PSA/NAMRIA vintage 2023 data supplied through BetterGov, not a verified direct GeoPortal download. Existing boundary table names are retained for compatibility. Coverage is partial until unmatched names are reviewed. Experimental spatial assignment retains the approximate boundary limitation and null confidence.

## Open

Review unmatched/ambiguous crosswalk entries against authoritative historical names and confirm polygon suitability before operational use. Do not silently strip aliases or repair invalid polygons.
