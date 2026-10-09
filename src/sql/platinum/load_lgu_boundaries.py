"""Load approved Geoportal city/municipality boundaries for spatial matching."""

from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime, timezone


def main() -> None:
    from pyspark.sql import SparkSession, functions as F

    catalog = os.environ.get("AHON_CATALOG", "ahon")
    property_name = os.environ.get("AHON_BOUNDARY_CODE_PROPERTY", "psgc_code")
    source_ref = os.environ.get(
        "AHON_BOUNDARY_GEOJSON_PATH",
        f"/Volumes/{catalog}/reference/source/lgu_boundaries/geoportal_city_municipality.geojson",
    )
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", property_name):
        raise ValueError("AHON_BOUNDARY_CODE_PROPERTY must be a simple GeoJSON property name")

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    raw = (
        spark.read.option("multiLine", "true")
        .json(source_ref)
        .select(F.explode("features").alias("feature"))
        .select(F.to_json("feature").alias("feature_json"))
    )
    if raw.limit(1).count() == 0:
        raise ValueError(f"No GeoJSON features found in {source_ref}")

    run_id = str(uuid.uuid4())
    loaded_at = datetime.now(timezone.utc)
    bronze = raw.select(
        "feature_json",
        F.lit("geoportal_city_municipality_boundary").alias("_source_name"),
        F.lit(source_ref).alias("_source_ref"),
        F.lit(loaded_at).cast("timestamp").alias("_ingested_at"),
        F.lit(run_id).alias("_batch_id"),
        F.sha2(F.col("feature_json"), 256).alias("_row_hash"),
    )
    bronze_table = f"{catalog}.bronze.geoportal_city_municipality_boundary"
    silver_table = f"{catalog}.silver.lgu_boundary_clean"
    bronze.write.format("delta").mode("overwrite").saveAsTable(bronze_table)

    silver = spark.table(bronze_table).select(
        F.get_json_object("feature_json", f"$.properties.{property_name}")
        .cast("string")
        .alias("psgc_code"),
        F.get_json_object("feature_json", "$.geometry").alias("boundary_geojson"),
        "_source_name",
        "_source_ref",
        "_ingested_at",
        "_batch_id",
        "_row_hash",
    )
    silver.write.format("delta").mode("overwrite").saveAsTable(silver_table)

    expected = (
        spark.table(f"{catalog}.reference.lgu_master")
        .filter(F.col("is_active") == F.lit(True))
        .filter(F.col("geographic_level").isin("City", "Municipality", "City/Municipality"))
        .select("psgc_code")
        .distinct()
    )
    invalid_codes = silver.filter(
        F.col("psgc_code").isNull() | ~F.col("psgc_code").rlike("^[0-9]{10}$")
    ).count()
    duplicate_codes = (
        silver.groupBy("psgc_code").count().filter(F.col("count") > 1).count()
    )
    unknown_codes = silver.select("psgc_code").distinct().join(
        expected, "psgc_code", "left_anti"
    ).count()
    malformed_geometry = silver.filter(
        F.col("boundary_geojson").isNull()
        | F.expr("try_to_geometry(boundary_geojson) IS NULL")
    ).count()
    details = json.dumps(
        {
            "source_ref": source_ref,
            "code_property": property_name,
            "invalid_codes": invalid_codes,
            "duplicate_codes": duplicate_codes,
            "unknown_or_inactive_codes": unknown_codes,
            "malformed_geometry": malformed_geometry,
        },
        sort_keys=True,
    )
    checks = [
        ("boundary_code_format", invalid_codes, "WARNING"),
        ("boundary_codes_unique", duplicate_codes, "BLOCKING"),
        ("boundary_code_active_city_municipality", unknown_codes, "WARNING"),
        ("boundary_geometry_valid", malformed_geometry, "BLOCKING"),
    ]
    result_rows = [
        (
            run_id,
            loaded_at.replace(tzinfo=None),
            "geoportal_city_municipality_boundary",
            silver_table,
            rule,
            severity,
            "FAIL" if failures and severity == "BLOCKING" else "WARN" if failures else "PASS",
            bronze.count(),
            int(failures),
            details,
        )
        for rule, failures, severity in checks
    ]
    schema = "run_id STRING, checked_at TIMESTAMP, dataset_name STRING, table_name STRING, rule_name STRING, severity STRING, status STRING, row_count LONG, failed_count LONG, details STRING"
    spark.createDataFrame(result_rows, schema).write.mode("append").saveAsTable(
        f"{catalog}.monitoring.dq_result"
    )
    if duplicate_codes or malformed_geometry:
        raise RuntimeError(
            "Boundary contract failed: "
            f"duplicate codes={duplicate_codes}, malformed geometry={malformed_geometry}"
        )
    print(f"Loaded {bronze.count()} approximate boundary features; {details}")


if __name__ == "__main__":
    main()
