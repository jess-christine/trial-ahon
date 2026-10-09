# TRIAL - AHON-Pipeline

Project AHON is a data-driven disaster and hazard monitoring platform designed to provide actionable insights that strengthen disaster preparedness and decision-making.

## Repository structure

```
ahon-pipeline/
├── README.md                    # this file
├── CONTRIBUTING.md              # how the team works: branches, PRs, review, commits
├── .gitignore                   # keeps secrets, local files and data out of git
├── databricks.yml               # Asset Bundle: dev and prod targets, variables, includes resources/
├── .github/
│   ├── workflows/               # CI checks and the reviewer auto-assign
│   ├── ISSUE_TEMPLATE/          # issue templates
│   └── pull_request_template.md # PR template
├── src/                         # pipeline code; SQL and Python files sit together
│   └── sql/                     # for now, all pipeline code sits under src/sql/ (may move up to src/ later)
│       ├── 00_setup/                               # shared: catalogs, schemas, volumes, control table
│       ├── datasets/                               # one folder per source dataset
│       │   └── publisher_dataset/                  # example: replace with the real dataset name
│       │       ├── extract/                        # optional: pulls data from the source and writes raw files
│       │       │   └── extract.py                  # example
│       │       ├── bronze/                         # raw data loaded into bronze.publisher_dataset
│       │       │   ├── load.sql                    # example
│       │       │   └── load_files.py               # example: Python only if the dataset needs it
│       │       └── silver/                         # cleaned data in silver.publisher_dataset_clean
│       │           ├── clean.sql                   # example
│       │           └── clean_rules.py              # example: Python only if the dataset needs it
│       ├── gold/                                   # shared: combines datasets into dimensions and facts
│       ├── platinum/                               # shared: analytics tables built from gold
│       ├── monitoring/                             # shared: data quality rules, run log
│       └── common/                                 # shared reusable Python
├── resources/                   # job and pipeline definitions (YAML), included by databricks.yml
├── notebooks/                   # experiments only; jobs never run these
│   └── exploration/
├── tests/                       # mirrors src/sql/; data quality checks sit next to the code they check
│   ├── datasets/
│   ├── gold/
│   ├── platinum/
│   └── common/
└── docs/
    ├── architecture/            # data model, source-to-target mapping, data-dictonary
    │   └── data-dictionary/     # what each table and column means: README.md is the index, one page per dataset
    ├── standards/               # naming standard
    ├── decisions/               # decision records, one per decision
    ├── governance/              # ownership
    ├── operations/              # monitoring, runbook, CI documentation check
    ├── getting-started/         # local setup for new team members
    └── stakeholder/             # guide for LGU users
```

### Where things go

- **Code:** under `src/sql/` for now (the `sql/` folder may be dropped later). Each dataset has its own folder under `src/sql/datasets/` with a `bronze/` and a `silver/` folder for its SQL, and Python where the dataset needs it. Code that is shared or combines datasets goes in the shared folders.
- **Dataset subfolders:** `bronze/` and `silver/` are the standard ones. If a dataset needs more, its owner can add a subfolder, such as `extract/` for code that pulls data from the source and writes the raw files. Keep the subfolder inside that dataset's folder, and describe it with a comment in this tree or a short note in the dataset's folder.
- **Source files:** files from file-based sources are uploaded to the team's Hugging Face dataset repository and copied, unchanged, into the `source` volume, which is in the `reference` schema (`ahon.reference.source`), by the dataset's `extract/` code, in one folder named after the dataset. Bronze code reads from the volume only. See [docs/decisions/0004-source-file-landing-hugging-face.md](docs/decisions/0004-source-file-landing-hugging-face.md).
- **Schemas:** folder names match the schema names in the naming standard, with no number prefixes.
- **Setup scripts:** files in `src/sql/00_setup/` have number prefixes because they must run in order.
- **Reusable Python:** in `src/sql/common/`, imported by the layer code.
- **Notebooks:** experiments only. Jobs run `.sql` and `.py` files from `src/sql/`.
- **Tests:** in `tests/`, in the same layout as `src/sql/`.
- **Docs:** in `docs/`. Decisions that shape the design are recorded in `docs/decisions/`.
- **Data dictionary:** each dataset has one page in `docs/architecture/data-dictionary/`, named after the dataset, describing its tables and columns. Add or update the page in the same PR that adds or changes the table.

The naming standard is in [docs/standards/naming.md](docs/standards/naming.md), and the team's way of working is in [CONTRIBUTING.md](CONTRIBUTING.md).

## Catalog structure

Two catalogs, one per environment, with the same schemas in each. Code moves between environments by changing only the catalog name.

```
CATALOG                    # ahon (production) or ahon_dev (development), with the same schemas in both
├── bronze                 # raw source tables: BLGF, CMCI, PHIVOLCS, PSA population, and PSGC
├── silver                 # source-specific typed and mapped tables; see the data dictionary
├── gold                   # target dimensions and facts: dim_lgu, dim_cmci_indicator, fact_cmci_indicator, fact_population, fact_ldrrmf, fact_earthquake_event
├── platinum               # analytics tables built from gold
├── source                 # pending: schema name still open, no volumes here for now (see below)
├── reference              # lookup and code tables, plus the `source` volume for raw files
└── monitoring             # control table, run log, and append-only dq_result table
```

- **Table names:** the bronze table is named after the dataset, and the silver table adds `_clean`, so `ahon.bronze.blgf_ldrrmf_annual_lgu` pairs with `ahon.silver.blgf_ldrrmf_annual_lgu_clean`. Gold tables start with `dim_` or `fact_`. The layer is never part of the name, because the schema already says it.
- **Raw files:** raw files live in the `source` volume, which is now in the `reference` schema (`ahon.reference.source`), one folder per dataset. The volume holds files, not tables. The separate `source` schema stays listed as pending until the team decides its final name or drops it.
- **Full rules:** see the [naming standard](docs/standards/naming.md) and [decision 0001](docs/decisions/0001-naming-standard.md).
- Bronze quality checks cover every implemented Bronze table and append rule counts to `ahon.monitoring.dq_result`; see [monitoring operations](docs/operations/monitoring.md).
- Silver source mappings are listed in the [data dictionary](docs/architecture/data-dictionary/README.md). Source-specific Silver jobs do not calculate risk or preparedness metrics; those are produced by the experimental Platinum layer.

## Databricks execution

Validate and deploy the [Databricks Asset Bundle](docs/operations/databricks-bundle.md) from the repository root. The manual `ahon_ingestion` job runs setup, source ingestion, boundary loading, and Bronze validation. The manual `ahon_medallion` job invokes ingestion, then runs Silver, Gold, Silver/Gold validation, and experimental Platinum. Serverless environment configuration, PSGC secret identifiers, reviewed CMCI mapping, landed source files, and the approved approximate boundary GeoJSON are required before a complete run.
