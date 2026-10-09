CREATE TABLE IF NOT EXISTS ahon.silver.psa_population_clean (
    geographic_location STRING,
    geographic_level STRING,
    region_name STRING,
    province_name STRING,
    lgu_name STRING,
    census_year INT,
    total_population BIGINT,
    urban_population BIGINT,
    percent_urban DOUBLE,
    _source_name STRING,
    _source_ref STRING,
    _ingested_at TIMESTAMP,
    _batch_id STRING,
    _row_hash STRING
)
USING DELTA;

MERGE INTO ahon.silver.psa_population_clean AS target
USING (
    WITH prepared AS (
        SELECT
            regexp_replace(geographic_location, '\\s+1/$', '') AS geographic_location,
            census_year,
            try_cast(try_cast(total_population AS DECIMAL(20, 2)) AS BIGINT) AS total_population,
            try_cast(try_cast(urban_population AS DECIMAL(20, 2)) AS BIGINT) AS urban_population,
            try_cast(percent_urban AS DOUBLE) AS percent_urban,
            _source_name,
            _source_ref,
            _ingested_at,
            _batch_id,
            _row_hash
        FROM ahon.bronze.psa_population_raw
    ),
    paths AS (
        SELECT *, split(geographic_location, ' > ') AS path_parts
        FROM prepared
    )
    SELECT
        geographic_location,
        CASE size(path_parts)
            WHEN 1 THEN 'National'
            WHEN 2 THEN 'Region'
            WHEN 3 THEN 'Province'
            WHEN 4 THEN 'City/Municipality'
            ELSE 'Unknown'
        END AS geographic_level,
        CASE WHEN size(path_parts) >= 2 THEN element_at(path_parts, 2) END AS region_name,
        CASE WHEN size(path_parts) >= 3 THEN element_at(path_parts, 3) END AS province_name,
        CASE WHEN size(path_parts) >= 4 THEN element_at(path_parts, 4) END AS lgu_name,
        try_cast(census_year AS INT) AS census_year,
        total_population,
        urban_population,
        percent_urban,
        _source_name,
        _source_ref,
        _ingested_at,
        _batch_id,
        _row_hash
    FROM paths
) AS source
ON target.geographic_location = source.geographic_location
AND target.census_year = source.census_year
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
