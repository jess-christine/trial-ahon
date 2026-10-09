CREATE TABLE IF NOT EXISTS ahon.silver.cmci_ingestion_batch_clean (
    batch_id STRING,
    requested_psgc_codes ARRAY<STRING>,
    requested_cmci_names ARRAY<STRING>,
    requested_years ARRAY<STRING>,
    requested_indicator_codes ARRAY<STRING>,
    expected_lgu_count INT,
    expected_year_count INT,
    expected_indicator_count INT,
    returned_value_count INT,
    response_hash STRING,
    ingestion_timestamp TIMESTAMP,
    is_complete BOOLEAN,
    _source_name STRING,
    _source_ref STRING,
    _ingested_at TIMESTAMP,
    _batch_id STRING,
    _row_hash STRING,
    silver_processed_timestamp TIMESTAMP
)
USING DELTA;

INSERT OVERWRITE TABLE ahon.silver.cmci_ingestion_batch_clean
SELECT
    batch_id,
    requested_psgc_codes,
    requested_cmci_names,
    requested_years,
    requested_indicator_codes,
    expected_lgu_count,
    expected_year_count,
    expected_indicator_count,
    returned_value_count,
    response_hash,
    ingestion_timestamp,
    size(requested_psgc_codes) = expected_lgu_count
        AND size(requested_cmci_names) = expected_lgu_count
        AND size(requested_years) = expected_year_count
        AND size(requested_indicator_codes) = expected_indicator_count
        AND returned_value_count = expected_lgu_count
            * expected_year_count
            * expected_indicator_count AS is_complete,
    _source_name,
    _source_ref,
    _ingested_at,
    _batch_id,
    _row_hash,
    current_timestamp() AS silver_processed_timestamp
FROM ahon.bronze.cmci_raw_indicator_batch_html;
