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
WITH parent_codes AS (
    SELECT DISTINCT trim(psgc_code) AS code, trim(version) AS source_version,
        trim(geographic_level) AS level
    FROM ahon.bronze.psgc
    WHERE trim(psgc_code) RLIKE '^[0-9]{10}$'
)
SELECT
    trim(source.psgc_code) AS psgc_code,
    trim(area_name) AS area_name,
    trim(source.geographic_level) AS geographic_level,
    region_parent.code AS region_code,
    province_parent.code AS province_code,
    municipality_parent.code AS municipality_code,
    CASE WHEN trim(source.geographic_level) = 'Bgy' AND trim(source.psgc_code) RLIKE '^[0-9]{10}$' THEN trim(source.psgc_code) END AS barangay_code,
    nullif(trim(correspondence_code), '') AS correspondence_code,
    nullif(trim(old_name), '') AS old_name,
    nullif(trim(city_class), '') AS city_class,
    nullif(trim(income_classification), '') AS income_classification,
    nullif(trim(urban_rural), '') AS urban_rural,
    nullif(trim(island_region), '') AS island_region,
    trim(status) AS status,
    trim(source.version) AS version,
    populations_json,
    _source_name,
    _source_ref,
    _ingested_at,
    _batch_id,
    _row_hash
<<<<<<< Updated upstream
FROM ahon.bronze.psgc
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY trim(psgc_code), trim(version)
    ORDER BY CASE WHEN trim(geographic_level) = 'Dist' THEN 1 ELSE 0 END
) = 1;
=======
FROM ahon.bronze.psgc source
LEFT JOIN parent_codes region_parent
    ON region_parent.code = concat(substr(trim(source.psgc_code), 1, 2), '00000000')
    AND region_parent.source_version = trim(source.version) AND region_parent.level = 'Reg'
LEFT JOIN parent_codes province_parent
    ON province_parent.code = concat(substr(trim(source.psgc_code), 1, 5), '00000')
    AND province_parent.source_version = trim(source.version) AND province_parent.level = 'Prov'
LEFT JOIN parent_codes municipality_parent
    ON municipality_parent.code = concat(substr(trim(source.psgc_code), 1, 7), '000')
    AND municipality_parent.source_version = trim(source.version)
    AND municipality_parent.level IN ('City', 'Mun');
>>>>>>> Stashed changes
