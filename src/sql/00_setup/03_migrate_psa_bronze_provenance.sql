-- One-time migration for PSA Bronze tables created before provenance columns
-- were aligned with the accepted leading-underscore naming standard.
ALTER TABLE ahon.bronze.psa_population_raw SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);

ALTER TABLE ahon.bronze.psa_population_raw RENAME COLUMN source_name TO _source_name;
ALTER TABLE ahon.bronze.psa_population_raw RENAME COLUMN source_ref TO _source_ref;
ALTER TABLE ahon.bronze.psa_population_raw RENAME COLUMN ingested_at TO _ingested_at;
ALTER TABLE ahon.bronze.psa_population_raw RENAME COLUMN batch_id TO _batch_id;
ALTER TABLE ahon.bronze.psa_population_raw RENAME COLUMN row_hash TO _row_hash;
