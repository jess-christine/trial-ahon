# PHILVOLCS Earthquake Seismic Activity

- **Source:** Philippine Institute of Volcanology and Seismology (PHIVOLCS), Department of Science and Technology (DOST). The raw extract is the combined CSV `phivolcs_earthquake_all_years.csv` generated from PHIVOLCS monthly earthquake pages and landed in the `ahon.reference.source` volume under the `philvolcs_earthquake` folder.
- **Coverage:** Monthly earthquake listings from 2019-01-01 through 2026-09-29 in the current extract, covering seismic events reported in the Philippines and nearby offshore areas; the source is refreshed as PHIVOLCS publishes new monthly tables.
- **Owner:** Department of Science and Technology - Philippine Institute of Volcanology and Seismology (DOST-Philvolcs)

## Bronze: `ahon.bronze.philvolcs_earthquake_data`

- **One row is:** one earthquake event as scraped from a PHIVOLCS monthly page: a timestamped event with latitude, longitude, depth, magnitude, and a free-text location description.
- **Loaded by:** `src/sql/datasets/philvolcs_earthquake/extract/philvolcs_earthquake_ingest.py` and `src/sql/datasets/philvolcs_earthquake/bronze/philvolcs_earthquake.sql`.
- **Row hash covers:** all eight source columns below, in the order listed, joined with `||` (empty strings remain empty).

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `Date-Time` | string | Earthquake event timestamp as published by PHIVOLCS, in the raw format `dd Month yyyy - hh:mm AM/PM` | Example: `31 January 2019 - 02:55 PM`; some rows have lowercase `am`/`pm` and a few non-event header rows also appear in the raw file |
| `Latitude` | double | Event latitude in decimal degrees north | Range in the current extract: 1.73 to 24.84; some missing values are present |
| `Longitude` | double | Event longitude in decimal degrees east | Range in the current extract: 116.3 to 132.18; some missing values are present |
| `Depth` | double | Earthquake depth in kilometers | Range in the current extract: 0.0 to 800.0; values are kept as scraped |
| `Magnitude` | double | Event magnitude | Range in the current extract: 1.0 to 7.8 |
| `Location` | string | Free-text location description, including distance and bearing from a known place | Examples include `025 km S 58Â° W of Sipalay (Negros Occidental)`. Degree symbols and formatting vary |
| `Month` | string | Month label from the source page, such as `January` | Used as scraped; the raw file also contains month/year banner rows |
| `Year` | int | Year value from the source page | Current extract spans 2019 to 2026 |
| `_source_name` | string | The dataset the row came from | Always `philvolcs_earthquake_data` |
| `_source_ref` | string | Raw source file path within the reference volume | Example: `/Volumes/ahon/reference/source/philvolcs_earthquake/phivolcs_earthquake_all_years.csv` |
| `_ingested_at` | timestamp (UTC) | When the row was loaded into bronze | |
| `_batch_id` | string | The run identifier that wrote the row | Same value on every row loaded in a single batch |
| `_row_hash` | string | SHA-256 of the raw source columns as received | Covers the eight source columns above, not the provenance columns |

The raw file in the current profile has 129,792 rows, of which 129,711 pass the silver validation rules after cleaning. Record counts by year in the valid set are: 2019 = 12,971; 2020 = 14,033; 2021 = 12,042; 2022 = 14,325; 2023 = 16,617; 2024 = 18,104; 2025 = 21,763; 2026 = 19,856.

## Silver: `ahon.silver.philvolcs_earthquake_data_clean`

- **One row is:** one valid earthquake event after parsing and validating the raw PHIVOLCS rows, with non-event banner rows removed and impossible values filtered out.
- **Key:** `id`
- **Built from bronze by:** `src/sql/datasets/philvolcs_earthquake/silver/philvolcs_earthquake_clean.sql`.

`src/sql/monitoring/bronze_quality.py` records timestamp parseability, coordinate and measure ranges, provenance, and exact-row-hash duplicates without changing Bronze. Source anomalies are warnings because the feed includes banner rows and incomplete measurements; the documented Silver validity rules determine which rows enter the analytic event table. Identical repeated events follow accepted decision [0009](../../decisions/0009-identical-earthquake-silver-events.md); conflicting attributes block replacement.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `id` | bigint | Stable surrogate key computed from `event_time`, `latitude`, `longitude`, `depth`, and `magnitude` | Derived via `xxhash64(...)` |
| `event_time` | timestamp | Parsed `Date-Time` value converted to a timestamp | Local PHIVOLCS time as shown in the source page; no timezone is attached in the source |
| `latitude` | double | Validated latitude in decimal degrees | Kept only when between -90 and 90 |
| `longitude` | double | Validated longitude in decimal degrees | Kept only when between -180 and 180 |
| `depth` | double | Event depth in kilometers | Kept only when depth is non-negative |
| `magnitude` | double | Event magnitude | Kept only when magnitude is non-negative |
| `location_description` | string | Cleaned free-text location description | `NULL` if blank after trimming |
| `month` | int | Numeric month extracted from `event_time` | Derived using `EXTRACT(MONTH FROM event_time)` |
| `year` | int | Numeric year extracted from `event_time` | Derived using `EXTRACT(YEAR FROM event_time)` |

## Known issues

- Bronze keeps every source row, including non-event banner rows and invalid values. Silver is the filtered analytic table; it does not delete or rewrite bronze and does not deduplicate rows unless a future business rule explicitly defines a unique-event key.
- The raw `Date-Time` field is a free-form string, and some values use lowercase `am`/`pm`; a small number of rows still fail parsing and are excluded in the clean layer.
- In the current source extract: 24 latitude values are missing or invalid, 24 longitude values are missing or invalid, 45 depth values are missing or invalid, and 3 magnitude values are missing. The silver validation rejects rows outside valid coordinate or non-negative-depth/magnitude limits.
-  Monthly PHIVOLCS HTML pages are scraped into a single combined CSV, so source formatting and raw row quality depend on the website layout and the scraper's parsing logic.
- Silver filters invalid current Bronze records, preserves historical events and collapses only identical observations under decision 0009.

## Open

- Confirm whether the source should store PHIVOLCS time as local time without timezone or whether it should be explicitly labeled as `Asia/Manila` in downstream datasets.
- Decide whether the raw `Month` and `Year` fields should be treated as trusted source metadata or replaced entirely by `event_time`-derived values.
- Review impossible event depths (for example, 800 km) and conflicting attributes sharing the deterministic event key; these are not automatically resolved.

## Silver invalid-record definition

A bronze row is invalid in silver if any of the following is true:

- `Date-Time` is blank or cannot be parsed as a timestamp.
- `Latitude` is null or outside the inclusive range [-90, 90].
- `Longitude` is null or outside the inclusive range [-180, 180].
- `Depth` is null or negative.
- `Magnitude` is null or negative.

A blank `Location` is allowed and becomes `NULL`; it does not make the record invalid.

## Accepted Silver duplicate cleanup

The existing `id` remains the event key. A candidate combines valid current Bronze
with retained Silver history. All event attributes must agree for repeated keys;
otherwise monitoring records a blocking failure and the atomic overwrite aborts.
For identical observations the latest ingestion timestamp wins, followed by batch
ID and row hash as deterministic tie-breakers. Bronze evidence is not rewritten.
Current valid Bronze key coverage is blocking; retained historical keys outside
current Bronze are a warning. The dev cleanup reduces 130,382 observations to
130,303 events without discarding distinct events. Reruns are idempotent at this
key, avoiding full source reloads and preserving selected-observation lineage.
