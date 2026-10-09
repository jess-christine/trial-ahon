CREATE SCHEMA IF NOT EXISTS ahon.bronze;

CREATE TABLE IF NOT EXISTS ahon.bronze.cmci_raw_indicator_batch_html (
    batch_id STRING NOT NULL,
    requested_psgc_codes ARRAY<STRING> NOT NULL,
    requested_cmci_names ARRAY<STRING> NOT NULL,
    requested_years ARRAY<STRING> NOT NULL,
    requested_indicator_codes ARRAY<STRING> NOT NULL,

    expected_lgu_count INT NOT NULL,
    expected_year_count INT NOT NULL,
    expected_indicator_count INT NOT NULL,
    returned_value_count INT NOT NULL,

    response_html STRING NOT NULL,
    response_hash STRING NOT NULL,
    ingestion_timestamp TIMESTAMP NOT NULL,

    -- Standard Bronze provenance
    _source_name STRING NOT NULL,
    _source_ref STRING NOT NULL,
    _ingested_at TIMESTAMP NOT NULL,
    _batch_id STRING NOT NULL,
    _row_hash STRING NOT NULL
)
USING DELTA
COMMENT 'Raw CMCI responses containing multiple LGUs, years, and indicators; one row per request batch';

CREATE TABLE IF NOT EXISTS ahon.bronze.cmci_raw_indicator (
    batch_id STRING NOT NULL,
    psgc_code STRING NOT NULL,
    psgc_name STRING,
    cmci_name STRING NOT NULL,
    indicator_label STRING NOT NULL,
    year STRING NOT NULL,
    raw_value STRING,

    response_hash STRING NOT NULL,
    ingestion_timestamp TIMESTAMP NOT NULL,

    -- Standard Bronze provenance
    _source_name STRING NOT NULL,
    _source_ref STRING NOT NULL,
    _ingested_at TIMESTAMP NOT NULL,
    _batch_id STRING NOT NULL,
    _row_hash STRING NOT NULL
)
USING DELTA
COMMENT 'Raw CMCI indicator values; one row per request batch, PSGC LGU, indicator, and year';
