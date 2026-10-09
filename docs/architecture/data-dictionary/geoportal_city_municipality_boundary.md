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
| `boundary_wkb_hex` | string | Hexadecimal WKB Polygon or MultiPolygon geometry | Coordinates must use WGS84 longitude/latitude (EPSG:4326) |
| `_source_name` | string | Source name | Carried from Bronze |
| `_source_ref` | string | Landed file path | Carried from Bronze |
| `_ingested_at` | timestamp (UTC) | Bronze load time | Carried from Bronze |
| `_batch_id` | string | Bronze load run | Carried from Bronze |
| `_row_hash` | string | SHA-256 of source feature | Carried from Bronze |

## Quality, limitations, and operation

Bronze DQ checks required raw feature content, geometry presence, provenance, hash format, and duplicate source feature candidates. Silver load checks code format, unique PSGC codes, active city/municipality membership, polygon geometry type, parseability, and WGS84 coordinate bounds. Malformed non-null codes, duplicate non-null codes, empty files, malformed/non-polygon geometry, or out-of-range coordinates block the load; unmatched null codes and unknown/inactive codes are warnings and are not silently removed.

Gold event matching uses native Databricks spatial functions and only populates `psgc_code` when exactly one active city/municipality polygon covers the point. Unmatched and overlapping-polygon events remain visible. The boundary source is explicitly approximate; event-to-LGU matches inherit that limitation and match confidence remains null because no calibrated confidence scale exists.

The GeoPortal inventory currently does not provide a downloadable boundary artifact. Before running, land an owner-approved GeoJSON FeatureCollection in the reference source volume and set its path and code-property name in the bundle. Runtime 17.1 or newer is required for native geometry functions. Processing avoids collecting polygons or event rows to the driver; the spatial predicate is a distributed join and should be monitored for runtime cost.

## Supplied GeoParquet preparation

The owner supplied `lgu_spatial.parquet` from [BetterGov dataset 23](https://data.bettergov.ph/datasets/23). The publisher describes curated PSA October 2023/NAMRIA November 2023 boundaries, not a current direct GeoPortal download. Keep that provenance and vintage limitation visible; existing table names remain unchanged.

`src/sql/06_reference/prepare_lgu_boundaries.py` converts this small reference artifact to the existing loader's GeoJSON input. Run locally with `--parquet`, `--psgc-workbook`, `--geojson`, and `--crosswalk` paths. Preparation requires PyArrow and Shapely locally; they are not added to the deployed job dependencies. It checks explicit EPSG:4326/WKB metadata and polygon validity. Existing outputs are never overwritten. Retain the original Parquet beside the generated GeoJSON in the source volume.

Mapping compares PH-prefixed identifiers with current PSGC and official historical correspondence codes, requiring exact name agreement (case and outer whitespace ignored). A globally unique exact reference name can resolve a changed code. No fuzzy matching, alias stripping, or manual code guesses are used. Unresolved or ambiguous codes remain null and are recorded in the crosswalk. All source attributes and original WKB bytes (hex) remain in GeoJSON properties, alongside mapping status/method/reference; SHA-256 covers the entire serialized feature. Bronze receives this explicitly prepared landing representation, not a claim of untouched provider GeoJSON. Silver geometry remains WGS84, and only valid active mapped keys can contribute to event assignment. Missing boundaries imply incomplete event assignment and proximity coverage.

## Runtime contract alignment

The supplied prepared GeoJSON uses `properties.psgc_code` for the reviewed
10-digit crosswalk and `properties.geometry_wkb_hex` for geometry. The bundle
selects `psgc_code`; it must not use the unrelated `PSGC` or original `PH...`
identifiers. Silver, Gold and Platinum consume WKB consistently through
`try_to_geometry(unhex(boundary_wkb_hex))`. Invalid geometry blocks the load;
individual unmatched keys remain warnings, but an entirely unmapped boundary
load is blocking. Platinum uses `st_distancespheroid` on WGS84 geometry for
centroid distances in metres, consistent with the accepted proximity definition.
Reruns use the retained source volume; no external boundary download is required.

WKB may omit the CRS tag even though coordinates are documented WGS84. Gold and
Platinum set the parsed polygon SRID to 4326 before comparing it with GeoJSON
points. This tags the documented coordinate system without reprojecting or
changing source coordinates, avoiding a 0-versus-4326 spatial-function error.
