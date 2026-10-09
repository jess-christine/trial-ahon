"""Build experimental city/municipality earthquake risk and preparedness outputs."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone


def _write(frame, table_name: str) -> None:
    frame.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(table_name)


def main() -> None:
    from pyspark.sql import SparkSession, Window, functions as F

    catalog = os.environ.get("AHON_CATALOG", "ahon")
    table = lambda schema, name: f"{catalog}.{schema}.{name}"
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    lgu = spark.table(table("gold", "dim_lgu")).filter(
        F.col("geographic_level").isin("City", "Municipality", "City/Municipality")
    ).select("psgc_code")
    population = spark.table(table("gold", "fact_population")).filter(
        F.col("psgc_code").isNotNull() & F.col("total_population").isNotNull()
    )
    latest_population_year = population.agg(F.max("year").alias("year")).first()["year"]
    if latest_population_year is None:
        raise RuntimeError("No matched city/municipality population rows are available")
    population = population.filter(F.col("year") == latest_population_year)
    population_window = Window.orderBy(F.col("total_population"), F.col("psgc_code"))
    population_score = population.select(
        "psgc_code",
        F.col("year").alias("population_year"),
        (F.percent_rank().over(population_window) * 100.0)
        .cast("decimal(7,2)")
        .alias("population_exposure_score"),
    )

    boundary = spark.table(table("silver", "lgu_boundary_clean")).select(
        "psgc_code", "boundary_geojson"
    ).join(lgu, "psgc_code", "inner")
    events = spark.table(table("gold", "fact_earthquake_event")).filter(
        F.col("timestamp").isNotNull()
        & F.col("longitude").between(-180, 180)
        & F.col("latitude").between(-90, 90)
        & F.col("magnitude").isNotNull()
    ).filter(
        (F.col("timestamp") >= F.add_months(F.current_timestamp(), -60))
        & (F.col("timestamp") <= F.current_timestamp())
    ).select("earthquake_fact_key", "timestamp", "magnitude", "longitude", "latitude")
    event_json = F.concat(
        F.lit('{"type":"Point","coordinates":['),
        F.col("longitude").cast("string"), F.lit(","),
        F.col("latitude").cast("string"), F.lit("]}"),
    )
    event_points = events.withColumn("_event_geojson", event_json)
    boundary_centroids = boundary.withColumn(
        "_centroid_geojson",
        F.expr("st_asgeojson(st_centroid(try_to_geometry(boundary_geojson)))"),
    )
    event_candidates = (
        event_points.crossJoin(boundary_centroids)
        .filter(
            F.expr(
                "st_contains(try_to_geometry(boundary_geojson), "
                "try_to_geometry(_event_geojson))"
            )
        )
        .withColumn(
            "_distance_m",
            F.expr(
                "st_distance(try_to_geography(_centroid_geojson), "
                "try_to_geography(_event_geojson))"
            ),
        )
        .select("earthquake_fact_key", "psgc_code", "magnitude", "_distance_m")
    )
    matched = event_candidates.groupBy("earthquake_fact_key").agg(
        F.countDistinct("psgc_code").alias("_match_count"),
        F.first("psgc_code").alias("_candidate_psgc_code"),
        F.first("magnitude").alias("magnitude"),
        F.first("_distance_m").alias("distance_m"),
    ).filter(F.col("_match_count") == 1)
    activity = matched.groupBy("_candidate_psgc_code").agg(
        F.countDistinct("earthquake_fact_key").alias("event_count"),
        F.avg("magnitude").alias("mean_magnitude"),
        F.avg("distance_m").alias("mean_centroid_distance_m"),
    ).withColumnRenamed("_candidate_psgc_code", "psgc_code")

    # Percentile components use the same city/municipality cohort and current 5-year window.
    activity_rank = Window.orderBy(F.col("event_count"), F.col("psgc_code"))
    magnitude_rank = Window.orderBy(F.col("mean_magnitude"), F.col("psgc_code"))
    proximity_rank = Window.orderBy(F.col("mean_centroid_distance_m").desc(), F.col("psgc_code"))
    activity_scores = activity.select(
        "psgc_code",
        (F.percent_rank().over(activity_rank) * 100.0).alias("_event_count_score"),
        (F.percent_rank().over(magnitude_rank) * 100.0).alias("_magnitude_score"),
        (F.percent_rank().over(proximity_rank) * 100.0).alias("_proximity_score"),
    ).select(
        "psgc_code",
        ((F.col("_event_count_score") + F.col("_magnitude_score") + F.col("_proximity_score")) / 3.0)
        .cast("decimal(7,2)")
        .alias("earthquake_activity_score"),
    )

    risk = (
        lgu.join(population_score, "psgc_code", "left")
        .join(activity_scores, "psgc_code", "left")
        .withColumn(
            "risk_score",
            (F.col("population_exposure_score") * F.col("earthquake_activity_score"))
            .cast("decimal(12,2)"),
        )
    )
    risk_window = Window.orderBy(F.col("risk_score"), F.col("psgc_code"))
    risk = risk.withColumn(
        "risk_tertile",
        F.when(F.col("risk_score").isNotNull(), F.ntile(3).over(risk_window)),
    ).withColumn(
        "risk_level",
        F.when(F.col("risk_tertile") == 3, "High")
        .when(F.col("risk_tertile") == 2, "Medium")
        .when(F.col("risk_tertile") == 1, "Low"),
    ).withColumn("risk_window_start", F.add_months(F.current_date(), -60))
    .withColumn("risk_window_end", F.current_date())
    .withColumn("population_year", F.lit(latest_population_year).cast("int"))
    _write(risk, table("platinum", "risk_level_by_lgu"))

    cmci = spark.table(table("gold", "fact_cmci_indicator")).filter(
        F.col("raw_score").isNotNull()
    )
    indicator_window = Window.partitionBy("year", "indicator_code").orderBy(
        F.col("raw_score"), F.col("psgc_code")
    )
    cmci = cmci.withColumn("indicator_percentile", F.percent_rank().over(indicator_window) * 100.0)
    latest_years = cmci.groupBy("psgc_code").agg(F.max("year").alias("cmci_year"))
    latest_cmci = cmci.alias("score").join(
        latest_years.alias("latest"),
        (F.col("score.psgc_code") == F.col("latest.psgc_code"))
        & (F.col("score.year") == F.col("latest.cmci_year")),
        "inner",
    ).select("score.*")
    expected_indicators = spark.table(table("gold", "dim_cmci_indicator")).count()
    cmci_capacity = latest_cmci.groupBy("psgc_code", "year").agg(
        F.countDistinct("indicator_code").alias("indicator_count"),
        F.avg("indicator_percentile").alias("cmci_capacity_score"),
    ).filter(F.col("indicator_count") == expected_indicators).select(
        "psgc_code", F.col("year").alias("cmci_year"),
        F.col("cmci_capacity_score").cast("decimal(7,2)"),
    )

    budget = spark.table(table("gold", "fact_ldrrmf")).filter(
        (F.col("psgc_code").isNotNull()) & F.col("utilization_rate").isNotNull()
    )
    latest_fiscal = budget.groupBy("psgc_code").agg(F.max("fiscal_year").alias("fiscal_year"))
    budget = budget.join(latest_fiscal, ["psgc_code", "fiscal_year"], "inner").groupBy(
        "psgc_code", "fiscal_year"
    ).agg(F.avg("utilization_rate").alias("ldrrmf_utilization_score"))
    preparedness = (
        risk.select("psgc_code", "risk_score", "risk_tertile")
        .join(cmci_capacity, "psgc_code", "left")
        .join(budget, "psgc_code", "left")
        .withColumn(
            "preparedness_score",
            F.when(
                F.col("cmci_capacity_score").isNotNull()
                & F.col("ldrrmf_utilization_score").isNotNull(),
                F.col("cmci_capacity_score") + F.col("ldrrmf_utilization_score"),
            ).cast("decimal(8,2)"),
        )
        .withColumn(
            "preparedness_gap",
            (F.col("preparedness_score") - F.col("risk_score")).cast("decimal(14,2)"),
        )
    )
    prep_window = Window.orderBy(F.col("preparedness_score"), F.col("psgc_code"))
    preparedness = preparedness.withColumn(
        "preparedness_tertile",
        F.when(F.col("preparedness_score").isNotNull(), F.ntile(3).over(prep_window)),
    ).withColumn(
        "preparedness_gap_rank",
        F.when(
            F.col("preparedness_gap").isNotNull(),
            F.rank().over(Window.orderBy(F.col("preparedness_gap").desc(), F.col("psgc_code"))),
        ),
    )
    _write(preparedness, table("platinum", "preparedness_gap_by_lgu"))

    vulnerability = preparedness.withColumn(
        "priority_category",
        F.when((F.col("risk_tertile") == 3) & (F.col("preparedness_tertile") == 1), "Highest priority")
        .when((F.col("risk_tertile") == 3) & (F.col("preparedness_tertile") > 1), "Monitor")
        .when((F.col("risk_tertile") < 3) & (F.col("preparedness_tertile") == 1), "Capacity concern")
        .when(F.col("risk_tertile").isNotNull() & F.col("preparedness_tertile").isNotNull(), "Lower priority"),
    )
    _write(vulnerability, table("platinum", "vulnerability_priority_by_lgu"))

    priority_lgus = vulnerability.filter(F.col("priority_category") == "Highest priority").select("psgc_code")
    priority_indicators = latest_cmci.join(priority_lgus, "psgc_code", "inner").groupBy(
        "year", "indicator_code"
    ).agg(
        F.avg("indicator_percentile").alias("mean_indicator_percentile"),
        F.countDistinct("psgc_code").alias("priority_lgu_count"),
    ).join(spark.table(table("gold", "dim_cmci_indicator")), "indicator_code", "inner")
    priority_indicators = priority_indicators.withColumn(
        "priority_rank",
        F.rank().over(Window.partitionBy("year").orderBy(F.col("mean_indicator_percentile"), F.col("indicator_code"))),
    ).select(
        F.col("year").alias("cmci_year"), "indicator_code", "pillar_name", "indicator_name",
        F.col("mean_indicator_percentile").cast("decimal(7,2)"), "priority_lgu_count", "priority_rank",
    )
    _write(priority_indicators, table("platinum", "cmci_preparedness_priority_by_indicator"))

    counts = {
        "active_lgus": lgu.count(),
        "population_scores": population_score.count(),
        "lgu_with_spatial_events": activity.count(),
        "risk_scores": risk.filter(F.col("risk_score").isNotNull()).count(),
        "preparedness_scores": preparedness.filter(F.col("preparedness_score").isNotNull()).count(),
        "highest_priority_lgus": priority_lgus.count(),
    }
    run_id = str(uuid.uuid4())
    checked_at = datetime.now(timezone.utc).replace(tzinfo=None)
    results = []
    for rule, failed_count, severity in (
        ("population_lgu_coverage", counts["active_lgus"] - counts["population_scores"], "WARNING"),
        ("earthquake_lgu_coverage", counts["active_lgus"] - counts["lgu_with_spatial_events"], "WARNING"),
        ("preparedness_lgu_coverage", counts["active_lgus"] - counts["preparedness_scores"], "WARNING"),
    ):
        results.append((run_id, checked_at, "platinum", table("platinum", "risk_level_by_lgu"), rule,
            severity, "WARN" if failed_count else "PASS", counts["active_lgus"], max(failed_count, 0),
            json.dumps(counts, sort_keys=True)))
    schema = "run_id STRING, checked_at TIMESTAMP, dataset_name STRING, table_name STRING, rule_name STRING, severity STRING, status STRING, row_count LONG, failed_count LONG, details STRING"
    spark.createDataFrame(results, schema).write.mode("append").saveAsTable(table("monitoring", "dq_result"))
    print(f"Experimental Platinum v1 complete: {json.dumps(counts, sort_keys=True)}")


if __name__ == "__main__":
    main()
