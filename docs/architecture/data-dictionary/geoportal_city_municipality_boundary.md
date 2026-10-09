# geoportal_city_municipality_boundary

- **Purpose:** provide approximate city/municipality polygons for assigning PHIVOLCS event coordinates to LGUs and supporting earthquake activity analysis.
- **Business question:** which city/municipality has recent earthquake activity and relative proximity to observed events?
- **Source:** the owner-approved Geoportal PH city/municipality boundary GeoJSON.
- **Coverage:** city/municipality polygons represented in the supplied GeoJSON file. Boundary date/version metadata is not yet extracted.
- **Owner:** AHON data owner.

## Bronze: `ahon.bronze.geoportal_city_municipality_boundary`

- **One row is:** one GeoJSON Feature as received from the current configured FeatureCollection file.
- **Loaded by:** `src/sql/platinum/load_lgu_boundaries.py`.
- **Key/hash:** idempotent source reference + SHA-256 of serialized feature JSON. Prior features remain when a source file changes; reruns of identical features do not duplicate rows.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `feature_json` | string | Complete serialized source feature | Retains GeoJSON properties and geometry as source evidence |
| `_source_name` | string | Source name | `geoportal_city_municipality_boundary` |
| `_source_ref` | string | Landed file path | Configured through `AHON_BOUNDARY_GEOJSON_PATH` |
| `_ingested_at` | timestamp (UTC) | Load time | |
| `_batch_id` | string | Load run | |
| `_row_hash` | string | SHA-256 of serialized feature | |

## Silver: `ahon.silver.geoportal_city_municipality_boundary_clean`

- **One row is:** one current city/municipality boundary feature.
- **Key:** PSGC code from the configured GeoJSON property, expected unique in the selected file.
- **Built from Bronze by:** `src/sql/platinum/load_lgu_boundaries.py`.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `psgc_code` | string | Source boundary's PSGC code | Property name configured by `AHON_BOUNDARY_CODE_PROPERTY`; never inferred from names |
| `boundary_geojson` | string | Polygon or MultiPolygon geometry | Coordinates must use WGS84 longitude/latitude (EPSG:4326) |
| `_source_name` | string | Source name | Carried from Bronze |
| `_source_ref` | string | Landed file path | Carried from Bronze |
| `_ingested_at` | timestamp (UTC) | Bronze load time | Carried from Bronze |
| `_batch_id` | string | Bronze load run | Carried from Bronze |
| `_row_hash` | string | SHA-256 of source feature | Carried from Bronze |

## Quality, limitations, and operation

Bronze DQ checks required raw feature content, geometry presence, provenance, hash format, and duplicate source feature candidates. Silver load checks code format, unique PSGC codes, active city/municipality membership, polygon geometry type, parseability, and WGS84 coordinate bounds. Missing/invalid codes, duplicate codes, empty files, malformed/non-polygon geometry, or out-of-range coordinates block the load; unknown/inactive codes are warnings and are not silently removed.

Gold event matching uses native Databricks spatial functions and only populates `psgc_code` when exactly one active city/municipality polygon covers the point. Unmatched and overlapping-polygon events remain visible. The boundary source is explicitly approximate; event-to-LGU matches inherit that limitation and match confidence remains null because no calibrated confidence scale exists.

The GeoPortal inventory currently does not provide a downloadable boundary artifact. Before running, land an owner-approved GeoJSON FeatureCollection in the reference source volume and set its path and code-property name in the bundle. Runtime 17.1 or newer is required for native geometry functions. Processing avoids collecting polygons or event rows to the driver; the spatial predicate is a distributed join and should be monitored for runtime cost.
