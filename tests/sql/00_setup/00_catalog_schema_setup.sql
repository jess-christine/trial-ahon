-- Create the ahon catalog
CREATE CATALOG IF NOT EXISTS ahon;

-- Create the project schemas used by the pipeline.
CREATE SCHEMA IF NOT EXISTS ahon.bronze;

CREATE SCHEMA IF NOT EXISTS ahon.silver;

CREATE SCHEMA IF NOT EXISTS ahon.gold;

CREATE SCHEMA IF NOT EXISTS ahon.platinum;

CREATE SCHEMA IF NOT EXISTS ahon.monitoring;

CREATE SCHEMA IF NOT EXISTS ahon.reference;

CREATE VOLUME IF NOT EXISTS ahon.reference.source;

SHOW SCHEMAS IN ahon;
