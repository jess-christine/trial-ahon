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

CREATE OR REPLACE TEMP VIEW phivolcs_event_candidate AS
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
<<<<<<< Updated upstream
    FROM deduped
    WHERE rn = 1
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;
=======
    FROM valid
UNION ALL
SELECT * FROM ahon.silver.philvolcs_earthquake_data_clean;

CREATE OR REPLACE TEMP VIEW phivolcs_event_conflicts AS
SELECT count(*) AS conflict_count
FROM (
    SELECT id FROM phivolcs_event_candidate GROUP BY id
    HAVING count(DISTINCT named_struct(
        'event_time', event_time, 'latitude', latitude, 'longitude', longitude,
        'depth', depth, 'magnitude', magnitude, 'location', location_description
    )) > 1
);

INSERT INTO ahon.monitoring.dq_result
SELECT uuid(), current_timestamp(), 'philvolcs_earthquake',
    'ahon.silver.philvolcs_earthquake_data_clean',
    'event_attributes_consistent_for_key', 'BLOCKING',
    CASE WHEN conflict_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    (SELECT count(*) FROM phivolcs_event_candidate), conflict_count,
    'Identical events may collapse; conflicting attributes block replacement'
FROM phivolcs_event_conflicts;

-- Retain historical Silver events even when the current Bronze snapshot shrinks.
INSERT OVERWRITE TABLE ahon.silver.philvolcs_earthquake_data_clean
SELECT
    CASE WHEN conflict_count = 0 THEN id
         ELSE cast(raise_error('PHIVOLCS event-key collision: conflicting attributes; Silver unchanged') AS BIGINT)
    END AS id,
    event_time, latitude, longitude, depth, magnitude, location_description,
    month, year, _source_name, _source_ref, _ingested_at, _batch_id, _row_hash
FROM (
    SELECT *, row_number() OVER (
        PARTITION BY id ORDER BY _ingested_at DESC, _batch_id DESC, _row_hash DESC
    ) AS event_rank
    FROM phivolcs_event_candidate
) ranked
CROSS JOIN phivolcs_event_conflicts
WHERE event_rank = 1;
>>>>>>> Stashed changes
