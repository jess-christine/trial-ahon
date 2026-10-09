# PSA Population

Raw 2024 Philippine Statistics Authority (PSA) census population data, loaded into the bronze layer of the `ahon` catalog.

## Summary

- 1,744 rows from the 2024 Census of Population (reference date 1 July 2024), loaded into `ahon.bronze.psa_population_raw`.
- Four geographic levels: 1 national, 18 regions, 118 provinces, 1,607 cities/municipalities.
- Source: PSA OpenSTAT PXWeb table `0241A6DPUP1.px`, saved as a CSV in `/Volumes/ahon/reference/source/psa_population/`.
- Loaded with a Delta `MERGE` on `geographic_location` + `census_year`; matched rows update only when `_row_hash` changes.
- All quality checks passed on the first load, and the regions sum to the national total (112,727,776).
- Silver removes the documented `1/` footnote marker from the national row, splits the geographic path, and casts numeric columns.

## How to reproduce

### Prerequisites

- Databricks workspace with Unity Catalog and a compute cluster or serverless compute that can run Python notebooks.
- Catalog `ahon` containing:
  - schema `reference` with a volume named `source` (path `/Volumes/ahon/reference/source`)
  - schema `bronze`
- Permissions: write access on the `source` volume, and `CREATE TABLE` plus `MODIFY` on `ahon.bronze`.
- Outbound network access from compute to `openstat.psa.gov.ph`.
- Python packages: `requests` (extract step). `pyspark` and `delta` are provided by the Databricks runtime. For CI linting, install `ruff`, `mypy`, `types-requests`, `pyspark` and `delta-spark`.

### Run

1. Deploy the plain Python extractor and loader with the [bundle](../../operations/databricks-bundle.md); they are Python script tasks, not imported notebooks.
2. Run the **extract task**. It POSTs a query selecting all geographic locations and all parameters from the PXWeb table, requests CSV, and writes the response unchanged to the volume path above. It prints the byte count, line count and CSV header.
3. Run the **load task**. It reads the CSV, builds the full geographic path for each row, adds provenance columns and a row hash, creates the bronze table if it does not exist, and merges into it. It prints the batch ID, rows merged, merge keys and column list.
4. Check the results against the expected values below.

### Configuration

The bundle supplies source endpoint, census year, volume root, and CSV encoding through explicit task configuration. Change deployment settings in `databricks.yml`; source column names, dataset grain, and hash coverage remain fixed by this dictionary.

| Setting | Value | Notes |
|---|---|---|
| `API_URL` | `https://openstat.psa.gov.ph/PXWeb/api/v1/en/DB/1A/PO_2024/0241A6DPUP1.px` | Extract task |
| `VOLUME_ROOT` | `/Volumes/ahon/reference/source` | Both tasks |
| `DATASET_NAME` | `psa_population` | One folder per dataset in the volume (team naming standard) |
| `SOURCE_PATH` | `<VOLUME_ROOT>/<DATASET_NAME>/2024_population_urban.csv` | Load task |
| `TABLE_NAME` | `ahon.bronze.psa_population_raw` | Load task |
| `CENSUS_YEAR` | `2024` | Set by the job; not present in the file |
| `CSV_ENCODING` | `cp1252` | Bundle `psa_csv_encoding`; the verified PXWeb response declares `text/csv; charset=Windows-1252`. Raw landing bytes are preserved; decoding is strict. |
| `MERGE_KEYS` | `geographic_location`, `census_year` | Load task |

The extractor checks the response charset against configured encoding before replacing the landed file. A changed charset, missing/extra CSV headers, or malformed record is a blocking contract failure. No replacement-character decoding or row filtering is used. UTF-8 sources must be explicitly configured as such.

### Re-running and idempotency

- Re-running with unchanged source data is safe: the merge matches every row and updates none, because `_row_hash` is identical. Row count stays the same.
- If PSA revises a value, only the affected rows are updated, and their provenance columns (`_ingested_at`, `_batch_id`, `_source_ref`) are refreshed.
- The extract step overwrites the CSV each run, so the volume always holds the latest pull. Keep a copy first if you need the previous version.
- To load another census year, update `API_URL` and `CENSUS_YEAR` and re-run both steps. Rows for different years do not overwrite each other, since `census_year` is part of the merge key.

### Expected results after a first load

| Check | Expected |
|---|---|
| Total rows | 1,744 |
| Distinct `census_year` | 1 (`2024`) |
| Rows by depth (1 / 2 / 3 / 4) | 1 / 18 / 118 / 1,607 |
| Duplicate `geographic_location` | 0 |
| Null or empty values | 0 in every column |
| Sum of region `total_population` | 112,727,776, equal to the national row |

The batch ID and `_ingested_at` differ on every run. The first load used batch `1fd78b5e-517e-402c-813c-4355211b0001`.

## Files ingested

| File | Location | Origin |
|---|---|---|
| `2024_population_urban.csv` | `/Volumes/ahon/reference/source/psa_population/` | PSA OpenSTAT PXWeb table `0241A6DPUP1.px` (2024 census: total population, urban population and percent urban by geographic location), pulled via the PXWeb API |

This is the only file ingested.

## Columns

All source columns are `STRING`; `_ingested_at` is `TIMESTAMP`. Values are stored as published by the loader; casting happens downstream.

### Source columns

| Column | Distinct | Description |
|---|---|---|
| `geographic_location` | 1,744 | Full hierarchical path joined with ` > ` (for example, region > province > city/municipality). Built from the dot-indentation in the source CSV (2 dots per level). The full path is needed because leaf names such as "San Isidro" repeat across provinces. |
| `total_population` | 1,726 | Total population of the area, as published by PSA. Values end in `.00`. |
| `urban_population` | 1,179 | Population living in urban areas within the area. Values end in `.00`. |
| `percent_urban` | 1,086 | Percentage of the area's population that is urban, on a 0 to 100 scale, with decimals. |
| `census_year` | 1 | Census year. `2024`. Set by the load job, not read from the file. |

### Provenance columns

| Column | Type | Description |
|---|---|---|
| `_source_name` | STRING | Source system. Always `PSA PXWeb API`. |
| `_source_ref` | STRING | Path of the file the row was read from. |
| `_ingested_at` | TIMESTAMP | When the batch that last wrote this row ran. |
| `_batch_id` | STRING | UUID of the load run that last wrote this row. |
| `_row_hash` | STRING | SHA-256 of the five source columns joined with `\|`. Used to detect changed rows. Unique per row (1,744 distinct). |

Because updates are gated on `_row_hash`, unchanged rows keep their original `_ingested_at`, `_batch_id` and `_source_ref`. These columns mean "last changed by", not "last seen by". Existing Delta tables created with non-underscored provenance fields must run `src/sql/00_setup/03_migrate_psa_bronze_provenance.sql` once before using the updated loader or validator.

## Silver: `ahon.silver.psa_population_clean`

- **One row is:** one PSA geographic path for a census year, with parsed numeric measures and explicit hierarchy fields.
- **Key:** `geographic_location + census_year`.
- **Built from Bronze by:** `src/sql/datasets/psa_population/silver/psa_population_clean.sql`.

Silver removes the documented `1/` footnote marker, splits the full path into region, province, and LGU names, assigns the source hierarchy level, and casts population measures to `BIGINT` and percent urban to `DOUBLE`. It carries Bronze provenance forward and retains every row; failed casts remain `NULL` and are visible in validation results. Gold uses only its city/municipality rows to build `fact_population`; it does not distribute municipal totals to barangays or include national, region, and province rows in that fact. See [fact_population](fact_population.md) for Gold grain and matching rules.

| Column | Type | Description | Notes |
|---|---|---|---|
| `geographic_location` | string | Full cleaned geographic path | Silver key part |
| `geographic_level` | string | National, region, province, city/municipality, or unknown path depth | |
| `region_name` | string | Region component of the path | `NULL` above region level |
| `province_name` | string | Province component of the path | `NULL` above province level |
| `lgu_name` | string | City/municipality component | `NULL` above city/municipality level |
| `census_year` | int | Census year | Silver key part |
| `total_population` | bigint | Total population | Failed cast remains `NULL` |
| `urban_population` | bigint | Urban population | Failed cast remains `NULL` |
| `percent_urban` | double | Percent of population in urban areas | 0 to 100 source scale |
| `_source_name` | string | Source system | Carried from Bronze |
| `_source_ref` | string | Source file | Carried from Bronze |
| `_ingested_at` | timestamp (UTC) | Bronze load time | Carried from Bronze |
| `_batch_id` | string | Bronze load run identifier | Carried from Bronze |
| `_row_hash` | string | Bronze raw-row hash | Carried from Bronze |

## Data profile (first load)

### Geographic hierarchy

| Depth | Level | Rows |
|---|---|---|
| 1 | National (PHILIPPINES) | 1 |
| 2 | Region | 18 |
| 3 | Province | 118 |
| 4 | City/Municipality | 1,607 |

### Numeric statistics (strings cast to double)

| Statistic | total_population | urban_population | percent_urban |
|---|---|---|---|
| Count | 1,744 | 1,744 | 1,744 |
| Mean | 244,710 | 129,477 | 25.6 |
| Std dev | 2,814,404 | 1,594,528 | 28.5 |
| Min | 406 | 0 | 0.0 |
| Max | 112,727,776 | 62,221,580 | 100.0 |

These statistics span all four levels, including the national row, so the mean and standard deviation are for orientation only. Do not use them as population measures.

### `percent_urban` distribution

| Bucket | Rows |
|---|---|
| 0% (fully rural) | 552 |
| 1 to 49% | 853 |
| 50 to 99% | 314 |
| 100% (fully urban) | 25 |

## Known issues and caveats

1. **Footnote marker in the national row.** The top-level name is `PHILIPPINES  1/`. The `1/` is a PSA footnote marker, and every path inherits it (for example, `PHILIPPINES  1/ > Region X (Northern Mindanao)`). Clean it in the silver layer.
2. **Repeated leaf names.** 122 leaf names appear under more than one province (`San Jose` and `San Isidro` 9 times each, `Santa Maria` and `Quezon` 7 times each). Never join or deduplicate on the leaf name alone. The full path is the unique key.
3. **All numeric columns are strings.** `total_population` and `urban_population` always end in `.00`, and 1,148 of 1,744 `percent_urban` values have decimals. Cast to numeric types in silver.
4. **Mixed hierarchy levels in one table.** Summing `total_population` across all rows multiple-counts the population. Filter by depth first.
5. **Long, redundant paths.** `geographic_location` runs 15 to 122 characters (average about 70). Split it into `region`, `province` and `lgu_name` columns in silver.
6. **Indentation dependency.** The hierarchy relies on 2 leading dots per level in the PXWeb CSV. If PSA changes that format, paths will be wrong without raising an error. The depth counts in the expected-results table are the quickest way to notice this.
7. **Merge key uniqueness.** Two source rows with the same path would make the Delta `MERGE` fail with a "multiple source rows matched" error. None exist today.
8. **Boundary and naming changes across censuses.** Administrative areas change between censuses (for example, the new Negros Island Region and the split of Maguindanao). Joining to 2020 or earlier data by name may not line up. Verify before comparing years.
9. **Official status.** The 2024 counts were declared official through Proclamation No. 973 (11 July 2025). The reference date is 1 July 2024.
