# LGU earthquake risk and preparedness dashboard

## Purpose and pages

`resources/ahon_risk_preparedness.lvdash.json` is the editable AI/BI dashboard definition for four city/municipality questions:

| Page | Business question | Visuals and measures |
| --- | --- | --- |
| Earthquake Risk Overview | What is each LGU's earthquake risk level? | Active/scored LGU counts, component percentiles, risk levels, actual top 20 scored LGUs, province summaries, LGU score/coverage table |
| Preparedness Gap Analysis | How does preparedness compare with risk? | CMCI, utilization, preparedness and gap scores; approved descending top 20 gaps; missing-component coverage; source years |
| Vulnerable LGUs | Which LGUs have high risk and low preparedness? | Highest-priority count, complete-score coverage, category counts, priority cohort and all-LGU details |
| Preparedness Priorities | Which preparedness aspects need attention? | Approved indicator count, scored priority count, indicator/pillar percentiles separated by CMCI year, cohort size and ranked indicator details |

## Data, grain and interpretation

The main dataset starts from every active city/municipality in `reference.lgu_master`, including PSA's `Mun` classification. It left joins Gold population and the four approved Platinum outputs by PSGC key. Preparedness and vulnerability joins also require the risk output's run ID, preventing partially updated snapshots being silently combined. Gold latest census population is displayed as context; the reference workbook population is not substituted into missing analytical scores. Region names come from the latest deterministically selected LDRRMF row and remain explicitly unavailable where no record supplies them.

Scores are consumed from Platinum, not recalculated in chart queries. Decision 0007's percentiles, five-year window, tertiles, descending gap and missing-component policy remain unchanged. Risk is 0–10,000 and preparedness combines different components; the gap is an experimental ordering aid, not a calibrated deficit. Utilization is already a percent-valued number, so the dashboard does not multiply it by 100 for display. Means describe scored LGUs only; coverage counts and statuses describe missing data. An unscored LGU is not zero-risk or low-priority.

The indicator page lists the approved Gold dimension even when the calculated highest-priority cohort is empty. Null priority scores/ranks remain null, with a coverage explanation. Indicator charts separate CMCI years, and do not manufacture a cohort. Top-20 queries limit eligible rows using deterministic row-number ordering, not a title-only promise. No geography, population allocation, or new business thresholds are introduced.

## Configure and update

The definition uses two-part schema/table names. Set the existing environment catalog with the Lakeview update API's `dataset_catalog` parameter or CLI `--dataset-catalog`; use the workspace's SQL warehouse ID. No machine path, warehouse ID, dashboard ID, credential, or environment catalog is embedded in the asset.

1. Get/export the current dashboard and save a backup outside the repository.
2. Prepare an update request with `serialized_dashboard` read from the asset, the current dashboard `etag`, and its warehouse ID.
3. Update the existing draft with `databricks lakeview update <dashboard-id> --dataset-catalog <catalog> --json @<request-file> --profile <profile>`.
4. Read the dashboard back and check all four pages, dataset defaults and widget counts. Publishing/credential sharing is a separate owner action.
5. Once compute is healthy, execute each dataset and inspect charts/tables, source years, null coverage and consistent run IDs. Refresh the open dashboard after the validated pipeline rebuild.

Updates use etag protection to preserve concurrent dashboard edits. This change updates the existing draft and does not publish or change grants. There is no extra scheduled extraction, materialized reporting layer, or added runtime dependency; queries reuse the small LGU/Platinum snapshots.

## Debugging and quality

A successful job result can be a partial task run; inspect enabled tasks and output counts before declaring end-to-end success. The inspected Gold logs contained zero CMCI fact rows and zero spatial event matches. The inspected Platinum attempt contained only 149 LGUs, zero risk scores and zero preparedness scores. Filters omitted `Mun`; they now accept that reviewed classification across boundary validation, spatial matching, LDRRMF references and Platinum. A blocking Platinum cohort rule records omitted LGUs before publishing outputs. Rebuild boundary/Gold, run Silver/Gold DQ, then rebuild Platinum after prerequisites are ready.

The SQL warehouse health API reported `Serverless warehouse cannot start ... workspace ... is no longer eligible for Serverless Compute`, zero clusters, and failed launch health. Administrator intervention to restore eligibility is required before live dataset query/rendering verification. The `RUNNING` warehouse state alone is insufficient. No extra warehouse was created to work around that eligibility failure, and our pending diagnostic queries were cancelled.

CMCI fact data must be ingested through the reviewed locality mapping and normal Bronze/Silver flow. Empty CMCI facts cannot produce trustworthy capacity/preparedness/priority scores. Do not fill them with population, fabricated values or zeroes. Coverage gaps, approximate boundary vintage, unmatched source identities and experimental metric limitations remain visible.

Local checks cover all widget dataset/field/encoding references, non-overlapping layout, portable catalog references, snapshot joins and actual top-20 restrictions. They do not substitute for SQL compilation and browser rendering in a healthy workspace.
