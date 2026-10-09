# fact_earthquake_event

- **Purpose:** retain validated PHIVOLCS earthquake observations for later hazard analysis.
- **Source and lineage:** `ahon.silver.philvolcs_earthquake_data_clean`, from the raw PHIVOLCS source rows. Silver preserves source fields and provenance; the Gold schema follows the supplied target model.
- **One row is:** one valid PHIVOLCS event observation.
- **Key:** `earthquake_fact_key`, using the Silver deterministic event key derived from time, coordinates, depth, and magnitude. Duplicate keys block Gold loading; identical source observations are deduplicated in Silver under decision [0009](../../decisions/0009-identical-earthquake-silver-events.md).
- **Built by:** `src/sql/gold/build_gold.py`; schema in `src/sql/00_setup/07_gold_setup.sql`.
- **Business questions:** provides valid earthquake event measures and approximate LGU assignment for experimental risk analysis under decision [0007](../../decisions/0007-experimental-platinum-v1.md).

| Column | Type | Description | Notes |
|---|---|---|---|
| `earthquake_fact_key` | bigint | Generated event observation key | Derived from Silver; duplicate keys block loading |
| `psgc_code` | varchar(10) | Matched LGU code | Spatially matched only when exactly one active city/municipality approximate polygon covers the event coordinates; otherwise `NULL` |
| `location` | varchar(500) | PHIVOLCS free-text location | No geocoding or inferred LGU assignment |
| `timestamp` | timestamp | Event time parsed from the PHIVOLCS source | Source has no timezone. Current Silver/Gold/Platinum jobs use UTC Spark sessions; confirm whether the source wall time is UTC or Asia/Manila before operational interpretation. |
| `depth` | decimal(10,2) | Event depth | Kilometers |
| `magnitude` | decimal(4,2) | Event magnitude | Source scale retained |
| `longitude` | decimal(10,7) | Event longitude | Decimal degrees |
| `latitude` | decimal(10,7) | Event latitude | Decimal degrees |
| `match_status` | psgc_match_status | PSGC matching state | `MATCHED`, `AMBIGUOUS`, or `UNMATCHED`; physical Databricks type is `STRING` |
| `match_confidence` | decimal(5,4) | Matching confidence | `NULL`; confidence is not calibrated for approximate boundaries |

## Quality and operations

Only rows already accepted by documented PHIVOLCS Silver validity rules are loaded. Source-to-Silver valid-row counts, Gold row counts, key uniqueness, coordinate ranges, and non-negative depth/magnitude are checked. Unmatched events warn and remain present. Gold is rebuilt from Silver; reruns do not accumulate duplicates. The five-year Platinum window currently compares parsed timestamps in UTC; timezone confirmation is required before interpreting events close to its cutoff.

The dev audit on 2026-10-09 found 74 repeated event keys (79 extra identical
observations). The owner subsequently authorized Silver cleanup under decision
0009. Gold checks Silver key uniqueness before its spatial join. Geometry is read
from `boundary_wkb_hex`; only scalar event/code columns pass to grouping, avoiding
unsupported Spark Connect geometry schema conversion. The matching rule is unchanged.
