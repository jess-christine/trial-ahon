# PHIVOLCS Earthquake Ingestion

This guide explains how to run the PHIVOLCS earthquake scraper and how its CSV output is used by the bronze and silver SQL jobs.

## Files

- Web Scraper: `src/sql/datasets/philvolcs_earthquake/extract/philvolcs_earthquake_ingest.py`
- Bronze load: `src/sql/datasets/philvolcs_earthquake/bronze/philvolcs_earthquake.sql`
- Silver clean: `src/sql/datasets/philvolcs_earthquake/silver/philvolcs_earthquake_clean.sql`
- Data dictionary: `docs/architecture/data-dictionary/philvolcs_earthquake_data.md`

## Before you run it

- Run the script in an environment where Python, `requests`, and `pandas` are installed. `urllib3` is optional.
- The runtime must be able to reach the PHIVOLCS website over HTTPS.
- The default output path is a Unity Catalog volume path:
  `/Volumes/ahon/reference/source/philvolcs_earthquake/`
- Run it on Databricks compute with access to that volume, or change `OUTPUT_DIR` in the script to a writable path available to the runtime.
- The identity running the script needs permission to write to the output directory.

## Run

From the repository root, execute:

```bash
python src/sql/datasets/philvolcs_earthquake/extract/philvolcs_earthquake_ingest.py
```

The script has no command-line arguments. Its `__main__` block sets `YEARS_TO_SCRAPE = 8` and calls `scrape_multiple_years`. The range is calculated relative to the year when it runs: eight calendar years including the current year. For example, in 2026 it requests 2019 through 2026.

To change the range or destination, update `YEARS_TO_SCRAPE` or `OUTPUT_DIR` in the `__main__` block before running. The functions can also be imported and called from Python:

```python
from src.sql.datasets.philvolcs_earthquake.extract.philvolcs_earthquake_ingest import (
    scrape_multiple_years,
)

combined_data, counts_by_year = scrape_multiple_years(
    years_back=8,
    output_dir="/Volumes/ahon/reference/source/philvolcs_earthquake/",
)
```

## Output

For each year with retrieved data, the script writes:

- `phivolcs_earthquake_<year>.csv`
- `phivolcs_earthquake_all_years.csv` — the combined data for the requested range

The combined CSV has these source columns: `Date-Time`, `Latitude`, `Longitude`, `Depth`, `Magnitude`, `Location`, `Month`, and `Year`. Files are written as UTF-8 with a byte-order mark. A run rewrites the files for years it retrieves and rewrites the combined file; it does not append to previous output. Existing annual files from years outside the newly requested range are not removed.

The combined file path must match the path configured in the bronze SQL. If you change `OUTPUT_DIR`, update the bronze SQL source path as well.

## What the scraper does

1. Requests each month's PHIVOLCS monthly page for every year in the configured range. It tries the PHIVOLCS main page only when the current UTC month's page returns HTTP 404; missing earlier months remain reported as gaps.
2. Parses the first suitable HTML table, labels its columns, and removes recognized page headers, summaries, and empty rows.
3. Adds `Month` and `Year`, then writes each year's CSV.
4. Concatenates the yearly results and writes the combined CSV.
5. Prints per-month progress and year/total row counts.

The summary ranks strongest events using numeric magnitude values without changing the source rows. Missing or malformed magnitude values remain in the landed CSV and are counted as omitted from the summary ranking only.

The scraper waits briefly between monthly requests. A failed month is reported in console output; the process may still produce files from months that succeeded. For the current year, the first failed month causes the remaining months to be skipped, assuming they have not been published yet. If no year returns data, no combined file is written.

## Load into the tables

After confirming the combined CSV was created at the expected volume path, run the bronze SQL to load source rows into `ahon.bronze.philvolcs_earthquake_data`, then run the silver SQL to build `ahon.silver.philvolcs_earthquake_data_clean`.

Bronze retains source rows. Silver parses timestamps, casts numeric values, filters invalid event rows, and carries the bronze provenance columns forward. Silver does not deduplicate observations.

## Troubleshooting

- **No output file:** Check the console for failed monthly requests, verify PHIVOLCS is reachable, and confirm the output directory is writable.
- **Bronze cannot find the combined CSV:** Compare the scraper's `OUTPUT_DIR` with the CSV path in `philvolcs_earthquake.sql`.
- **Some months are absent:** Review the scraper output for HTTP failures or empty tables. The scraper does not fail the entire run just because one month has no retrieved data.
- **A later scrape has different counts:** The source pages can change and the scraper replaces outputs for the requested range. Compare the new CSV and reported counts before running the bronze and silver jobs.

## Operational note

The scraper currently disables TLS certificate verification on its HTTP sessions. This weakens protection against an intercepted or spoofed connection. Before relying on this ingestion for production data, restore normal certificate verification and investigate any certificate errors rather than disabling verification.
