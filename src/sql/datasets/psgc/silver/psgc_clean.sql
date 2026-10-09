CREATE TABLE IF NOT EXISTS ahon.silver.psgc_clean (
    psgc_code STRING,
    area_name STRING,
    geographic_level STRING,
    region_code STRING,
    province_code STRING,
    municipality_code STRING,
    barangay_code STRING,
    correspondence_code STRING,
    old_name STRING,
    city_class STRING,
    income_classification STRING,
    urban_rural STRING,
    island_region STRING,
    status STRING,
    version STRING,
    populations_json STRING,
    _source_name STRING,
    _source_ref STRING,
    _ingested_at TIMESTAMP,
    _batch_id STRING,
    _row_hash STRING
)
USING DELTA;

INSERT OVERWRITE TABLE ahon.silver.psgc_clean
SELECT
    trim(psgc_code) AS psgc_code,
    trim(area_name) AS area_name,
    trim(geographic_level) AS geographic_level,
    nullif(trim(region_code), '') AS region_code,
    nullif(trim(province_code), '') AS province_code,
    nullif(trim(municipality_code), '') AS municipality_code,
    nullif(trim(barangay_code), '') AS barangay_code,
    nullif(trim(correspondence_code), '') AS correspondence_code,
    nullif(trim(old_name), '') AS old_name,
    nullif(trim(city_class), '') AS city_class,
    nullif(trim(income_classification), '') AS income_classification,
    nullif(trim(urban_rural), '') AS urban_rural,
    nullif(trim(island_region), '') AS island_region,
    trim(status) AS status,
    trim(version) AS version,
    populations_json,
    _source_name,
    _source_ref,
    _ingested_at,
    _batch_id,
    _row_hash
FROM ahon.bronze.psgc
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY trim(psgc_code), trim(version)
    ORDER BY CASE WHEN trim(geographic_level) = 'Dist' THEN 1 ELSE 0 END
) = 1;
