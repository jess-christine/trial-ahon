CREATE TABLE IF NOT EXISTS ahon.silver.philvolcs_earthquake_data_clean (
    id BIGINT NOT NULL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    depth DOUBLE NOT NULL,
    magnitude DOUBLE NOT NULL,
    location_description STRING,
    month INT NOT NULL,
    year INT NOT NULL,
    _source_name STRING,
    _source_ref STRING,
    _ingested_at TIMESTAMP,
    _batch_id STRING,
    _row_hash STRING
);

MERGE INTO ahon.silver.philvolcs_earthquake_data_clean AS target
USING (
    WITH parsed AS (
        SELECT
            bronze._source_name,
            bronze._source_ref,
            bronze._ingested_at,
            bronze._batch_id,
            bronze._row_hash,
            try_to_timestamp(bronze.`Date-Time`, 'dd MMMM yyyy - hh:mm a') AS event_time,
            try_cast(bronze.Latitude AS DOUBLE) AS latitude,
            try_cast(bronze.Longitude AS DOUBLE) AS longitude,
            try_cast(bronze.Depth AS DOUBLE) AS depth,
            try_cast(bronze.Magnitude AS DOUBLE) AS magnitude,
            nullif(trim(bronze.Location), '') AS location_description
        FROM ahon.bronze.philvolcs_earthquake_data AS bronze
        WHERE trim(bronze.`Date-Time`) <> ''
    ),
    invalid AS (
        SELECT
            *,
            CASE
                WHEN event_time IS NULL THEN 1
                WHEN latitude IS NULL OR latitude < -90 OR latitude > 90 THEN 1
                WHEN longitude IS NULL OR longitude < -180 OR longitude > 180 THEN 1
                WHEN depth IS NULL OR depth < 0 THEN 1
                WHEN magnitude IS NULL OR magnitude < 0 THEN 1
                ELSE 0
            END AS is_invalid
        FROM parsed
    ),
    valid AS (
        SELECT
            *
        FROM invalid
        WHERE is_invalid = 0
    ),
    hashed AS (
        SELECT
            xxhash64(event_time, latitude, longitude, depth, magnitude) AS id,
            event_time,
            latitude,
            longitude,
            depth,
            magnitude,
            location_description,
            extract(MONTH FROM event_time) AS month,
            extract(YEAR FROM event_time) AS year,
            _source_name,
            _source_ref,
            _ingested_at,
            _batch_id,
            _row_hash
        FROM valid
    ),
    deduped AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) AS rn
        FROM hashed
    )
    SELECT
        id,
        event_time,
        latitude,
        longitude,
        depth,
        magnitude,
        location_description,
        month,
        year,
        _source_name,
        _source_ref,
        _ingested_at,
        _batch_id,
        _row_hash
    FROM deduped
    WHERE rn = 1
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;