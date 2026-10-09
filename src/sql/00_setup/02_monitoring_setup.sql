CREATE SCHEMA IF NOT EXISTS ahon.monitoring;

CREATE TABLE IF NOT EXISTS ahon.monitoring.dq_result (
    run_id STRING NOT NULL,
    checked_at TIMESTAMP NOT NULL,
    dataset_name STRING NOT NULL,
    table_name STRING NOT NULL,
    rule_name STRING NOT NULL,
    severity STRING NOT NULL,
    status STRING NOT NULL,
    row_count BIGINT NOT NULL,
    failed_count BIGINT NOT NULL,
    details STRING
)
USING DELTA
COMMENT 'Append-only results from read-only Bronze quality validation runs';
