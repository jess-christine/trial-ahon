CREATE TABLE IF NOT EXISTS ahon.bronze.philvolcs_earthquake_data (
    `Date-Time` STRING,
    Latitude DOUBLE,
    Longitude DOUBLE,
    Depth DOUBLE,
    Magnitude DOUBLE,
    Location STRING,
    Month STRING,
    Year INT,
    _source_name STRING,
    _source_ref STRING,
    _ingested_at TIMESTAMP,
    _batch_id STRING,
    _row_hash STRING
);

WITH run_context AS (
    SELECT uuid() AS batch_id
), source_rows AS (
    SELECT
        raw.`Date-Time` AS `Date-Time`,
        raw.Latitude AS Latitude,
        raw.Longitude AS Longitude,
        raw.Depth AS Depth,
        raw.Magnitude AS Magnitude,
        raw.Location AS Location,
        raw.Month AS Month,
        raw.Year AS Year,
        'philvolcs_earthquake_data' AS _source_name,
        '/Volumes/ahon/reference/source/philvolcs_earthquake/phivolcs_earthquake_all_years.csv' AS _source_ref,
        current_timestamp() AS _ingested_at,
        CAST(context.batch_id AS STRING) AS _batch_id,
        sha2(concat_ws(
            '||',
            coalesce(CAST(raw.`Date-Time` AS STRING), ''),
            coalesce(CAST(raw.Latitude AS STRING), ''),
            coalesce(CAST(raw.Longitude AS STRING), ''),
            coalesce(CAST(raw.Depth AS STRING), ''),
            coalesce(CAST(raw.Magnitude AS STRING), ''),
            coalesce(CAST(raw.Location AS STRING), ''),
            coalesce(CAST(raw.Month AS STRING), ''),
            coalesce(CAST(raw.Year AS STRING), '')
        ), 256) AS _row_hash
    FROM read_files(
        '/Volumes/ahon/reference/source/philvolcs_earthquake/phivolcs_earthquake_all_years.csv',
        format => 'csv',
        header => true,
        inferColumnTypes => true
    ) AS raw
    CROSS JOIN run_context AS context
)
INSERT OVERWRITE TABLE ahon.bronze.philvolcs_earthquake_data
SELECT * FROM source_rows;
