# blgf_ldrrmf_annual_lgu

- **Source:** Bureau of Local Government Finance (BLGF), LGU time series data (https://blgf.gov.ph/lgu-timeseries-data/). One Excel file per fiscal year, `FY2018-LDRRMF-by-LGU.xlsx` to `FY2024-LDRRMF-by-LGU.xlsx`, landed in the `source` volume (`ahon.reference.source`) through Hugging Face (see [0004](../../decisions/0004-source-file-landing-hugging-face.md)).
- **Coverage:** fiscal years 2018 to 2024, one file per year. One line per province, city and municipality. No barangay level.
- **Owner:** to be filled in (see [ownership](../../governance/ownership.md)).

What it measures: each LGU's yearly budget (appropriation) and actual spending (expenditure) of its Local Disaster Risk Reduction and Management Fund (LDRRMF), split into the 70% LDRRMF part and the 30% Quick Response Fund, plus totals. Amounts are in pesos.

## Bronze: `ahon.bronze.blgf_ldrrmf_annual_lgu`

- **One row is:** one line of a BLGF Excel file: one LGU (province, city or municipality) in one fiscal year, as received. All seven files are stacked in one table, so the same LGU appears once per year.
- **Loaded by:** `src/sql/datasets/blgf_ldrrmf_annual_lgu/extract/` copies the files into the volume, and `src/sql/datasets/blgf_ldrrmf_annual_lgu/bronze/load_blgf_ldrrmf_annual_lgu.py` reads them into this table. A run rebuilds the whole table from the files (overwrite), so rerunning does not duplicate rows.
- **Row hash covers:** the ten source columns below, in the order listed, joined with `||` (an empty cell counts as an empty string).
- **No year column:** the fiscal year is only in the file name, which `_source_ref` holds (the digits after `FY`). Silver derives it from there.

| Column | Type | Description | Notes |
| --- | --- | --- | --- |
| `region` | string | Region the LGU belongs to | Source header `REGION`, kept as received |
| `province` | string | Province the LGU belongs to | Source header `PROVINCE` |
| `lgu_name` | string | Name of the province, city or municipality | Source header `LGU NAME`. The same LGU may be spelled differently between years (not yet checked) |
| `lgu_type` | string | Whether the line is a province, a city or a municipality | Source header `LGU TYPE` (labelled `LGU CODE` in FY2021, but it holds the type). Values: `Province`, `City`, `Municipality` |
| `70pct_ldrrmf_budget_appropriation` | double | Budget appropriation for the 70% LDRRMF part | Pesos. Source header `70% LDRRMF` / `Budget Appropriation` |
| `70pct_ldrrmf_expenditures` | double | Amount spent from the 70% LDRRMF part | Pesos. `70% LDRRMF` / `Expenditures` |
| `30pct_quick_response_fund_budget_appropriation` | double | Budget appropriation for the 30% Quick Response Fund | Pesos. `30% QUICK RESPONSE FUND` / `Budget Appropriation` |
| `30pct_quick_response_fund_expenditures` | double | Amount spent from the 30% Quick Response Fund | Pesos. `30% QUICK RESPONSE FUND` / `Expenditures` |
| `total_budget_appropriation` | double | Total budget appropriation of the fund | Pesos. `TOTAL` / `Budget Appropriation`. Equals the sum of the two parts in every row |
| `total_expenditures` | double | Total amount spent | Pesos. `TOTAL` / `Expenditures`. Equals the sum of the two parts in every row |
| `_source_name` | string | The dataset the row came from | Always `blgf_ldrrmf_annual_lgu` |
| `_source_ref` | string | Where the row was loaded from: the path of the Excel file in the volume | For example `/Volumes/ahon_dev/reference/source/blgf_ldrrmf_annual_lgu/FY2018-LDRRMF-by-LGU.xlsx` |
| `_ingested_at` | timestamp (UTC) | When the row was loaded into bronze | |
| `_batch_id` | string | The load run that wrote the row | A random id for each run, the same on every row of that run |
| `_row_hash` | string | SHA-256 of the raw source columns as received | Covers the ten source columns above, not the provenance columns |

Rows per file: 1,513 (FY2018), 1,566, 1,612, 1,637, 1,683, 1,707, 1,716 (FY2024), 11,434 in total. The load stops without writing if a file's count differs.

## Silver: `ahon.silver.blgf_ldrrmf_annual_lgu_clean`

- **One row is:** one Bronze report row, typed for downstream joins and analysis.
- **Key:** no PSGC key is assigned; the source has no PSGC code and locality matching has not been reviewed.
- **Built from Bronze by:** `src/sql/datasets/blgf_ldrrmf_annual_lgu/silver/blgf_ldrrmf_annual_lgu_clean.sql`.

Fiscal year is parsed from the filename in `_source_ref`; amount columns are cast to `DECIMAL(18,2)`. Failed casts remain `NULL` and appear in Bronze validation results. The snapshot is overwritten from Bronze on each run so source duplicates are retained and reruns are idempotent. No utilization rate, PSGC match, or preparedness score is derived here.

The Silver schema contains `fiscal_year INT`, the four location/type fields as `STRING`, the six appropriation/expenditure fields as `DECIMAL(18,2)`, and the five Bronze provenance fields unchanged. Its row lineage is the `_row_hash` and `_source_ref` pair; it does not claim a unique LGU key.

## Known issues

- **The Excel files do not share one layout.** The data sheet name and position change by year, the header sits on a different row in FY2024, FY2023 puts `LGU TYPE` first, and FY2021 labels it `LGU CODE`. The load finds the header row and maps columns by name, and stops with an error if a file does not fit.
- **Zero may mean "not reported".** 187 rows have a zero total budget (62, 50, 26, 28, 9, 5, 7 for FY2018 to FY2024). Bronze has no nulls, so a blank and a real zero look the same.
- **Spending above budget.** 388 rows (3.4%) have expenditure above appropriation (98, 85, 107, 56, 23, 10, 9). It may be legitimate (carry-over or supplemental funds) or an entry error. Kept as received.
- **Province rows are separate funds** from city and municipality rows. Do not add them together.
- **Coverage grows over time**, from 1,513 lines in FY2018 to 1,716 in FY2024.
- **No PSGC code.** Matching to other datasets has to use region, province and LGU name, and municipality names repeat across provinces.
- **Only the data sheet is loaded.** Each Excel file also has a `Metadata` sheet (originator, extraction date, disclaimer), which is not loaded.

## Open

- What the 70% and 30% parts mean in BLGF's own notes (by law the 70% covers mitigation and preparedness and the 30% is the Quick Response Fund, not yet checked against BLGF), and whether "expenditures" means cash paid out or obligations.
- Final catalog, schema and volume names. The dev workspace uses `ahon_dev`.
- Column naming rules are still open in the [naming standard](../../standards/naming.md), so these are the names as they exist in the table.
- The silver table (`blgf_ldrrmf_annual_lgu_clean`) is not built yet. Add its section here when it is.
