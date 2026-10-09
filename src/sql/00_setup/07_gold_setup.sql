CREATE SCHEMA IF NOT EXISTS ahon.gold;

CREATE TABLE IF NOT EXISTS ahon.gold.dim_lgu (
    psgc_code VARCHAR(10) NOT NULL,
    area_name VARCHAR(255) NOT NULL,
    geographic_level STRING NOT NULL,
    region_code VARCHAR(10),
    province_code VARCHAR(10),
    municipality_code VARCHAR(10),
    barangay_code VARCHAR(10)
)
USING DELTA
COMMENT 'Active LGUs from the reviewed master, enriched by exact PSGC-code hierarchy matches';

CREATE TABLE IF NOT EXISTS ahon.gold.dim_cmci_indicator (
    indicator_code VARCHAR(10) NOT NULL,
    pillar_name VARCHAR(50) NOT NULL,
    indicator_name VARCHAR(255) NOT NULL
)
USING DELTA
COMMENT 'Approved CMCI indicator codes and names from the shared ingestion configuration';

CREATE TABLE IF NOT EXISTS ahon.gold.fact_cmci_indicator (
    psgc_code VARCHAR(10) NOT NULL,
    year INT NOT NULL,
    indicator_code VARCHAR(10) NOT NULL,
    psgc_name VARCHAR(255),
    cmci_name VARCHAR(255),
    geographic_level VARCHAR(50),
    raw_score DECIMAL(18, 6)
)
USING DELTA
COMMENT 'One row per approved PSGC LGU, CMCI year, and indicator; raw source score is not normalized';

CREATE TABLE IF NOT EXISTS ahon.gold.fact_population (
    geographic_location STRING NOT NULL,
    psgc_code VARCHAR(10),
    year INT NOT NULL,
    total_population BIGINT,
    urban_population BIGINT,
    percent_urban DOUBLE
)
USING DELTA
COMMENT 'One PSA city/municipality population row per source geographic path and census year; exact LGU/province matches may link to dim_lgu';

CREATE TABLE IF NOT EXISTS ahon.gold.fact_ldrrmf (
    ldrrmf_fact_key BIGINT NOT NULL,
    psgc_code VARCHAR(10),
    fiscal_year INT,
    region VARCHAR(255),
    province VARCHAR(255),
    lgu_name VARCHAR(255),
    lgu_type VARCHAR(50),
    appropriation_70_pct DECIMAL(18, 2),
    expenditure_70_pct DECIMAL(18, 2),
    appropriation_30_pct DECIMAL(18, 2),
    expenditure_30_pct DECIMAL(18, 2),
    total_appropriation DECIMAL(18, 2),
    total_expenditure DECIMAL(18, 2),
    utilization_rate DECIMAL(7, 2),
    match_status STRING,
    match_confidence DECIMAL(5, 4)
)
USING DELTA
COMMENT 'One original BLGF report row per city/municipality and fiscal year; exact active-reference name matches only';

CREATE TABLE IF NOT EXISTS ahon.gold.fact_earthquake_event (
    earthquake_fact_key BIGINT NOT NULL,
    psgc_code VARCHAR(10),
    location VARCHAR(500),
    timestamp TIMESTAMP,
    depth DECIMAL(10, 2),
    magnitude DECIMAL(4, 2),
    longitude DECIMAL(10, 7),
    latitude DECIMAL(10, 7),
    match_status STRING,
    match_confidence DECIMAL(5, 4)
)
USING DELTA
COMMENT 'One valid PHIVOLCS observation; geographic matches use a unique approved approximate municipal boundary';
