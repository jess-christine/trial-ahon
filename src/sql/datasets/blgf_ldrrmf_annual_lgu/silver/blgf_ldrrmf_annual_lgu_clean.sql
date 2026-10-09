CREATE TABLE IF NOT EXISTS ahon.silver.blgf_ldrrmf_annual_lgu_clean (
    fiscal_year INT,
    region STRING,
    province STRING,
    lgu_name STRING,
    lgu_type STRING,
    `70pct_ldrrmf_budget_appropriation` DECIMAL(18, 2),
    `70pct_ldrrmf_expenditures` DECIMAL(18, 2),
    `30pct_quick_response_fund_budget_appropriation` DECIMAL(18, 2),
    `30pct_quick_response_fund_expenditures` DECIMAL(18, 2),
    total_budget_appropriation DECIMAL(18, 2),
    total_expenditures DECIMAL(18, 2),
    _source_name STRING,
    _source_ref STRING,
    _ingested_at TIMESTAMP,
    _batch_id STRING,
    _row_hash STRING
)
USING DELTA;

INSERT OVERWRITE TABLE ahon.silver.blgf_ldrrmf_annual_lgu_clean
SELECT
    try_cast(regexp_extract(_source_ref, 'FY([0-9]{4})-', 1) AS INT) AS fiscal_year,
    region,
    province,
    lgu_name,
    lgu_type,
    try_cast(`70pct_ldrrmf_budget_appropriation` AS DECIMAL(18, 2)),
    try_cast(`70pct_ldrrmf_expenditures` AS DECIMAL(18, 2)),
    try_cast(`30pct_quick_response_fund_budget_appropriation` AS DECIMAL(18, 2)),
    try_cast(`30pct_quick_response_fund_expenditures` AS DECIMAL(18, 2)),
    try_cast(total_budget_appropriation AS DECIMAL(18, 2)),
    try_cast(total_expenditures AS DECIMAL(18, 2)),
    _source_name,
    _source_ref,
    _ingested_at,
    _batch_id,
    _row_hash
FROM ahon.bronze.blgf_ldrrmf_annual_lgu;
