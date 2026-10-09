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
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as F

    catalog = os.environ.get("AHON_CATALOG", "ahon")

    def table(schema: str, name: str) -> str:
        return f"{catalog}.{schema}.{name}"
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    run_id = str(uuid.uuid4())
    calculated_at = datetime.now(timezone.utc).replace(tzinfo=None)

    lgu = spark.table(table("gold", "dim_lgu")).filter(
        F.col("geographic_level").isin("City", "Municipality", "City/Municipality")
    ).select("psgc_code")
    population = spark.table(table("gold", "fact_population")).filter(
        F.col("psgc_code").isNotNull()
    )
    if (
        population.groupBy("psgc_code", "year")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    ):
        raise RuntimeError("Matched population must be unique per LGU and census year")
    population = population.filter(F.col("total_population").isNotNull())
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

    boundary = spark.table(
        table("silver", "geoportal_city_municipality_boundary_clean")
    ).select(
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
    ).withColumn("_boundary_geom", F.expr("try_to_geometry(boundary_geojson)"))
    for coordinate, function_name in (
        ("_xmin", "st_xmin"),
        ("_xmax", "st_xmax"),
        ("_ymin", "st_ymin"),
        ("_ymax", "st_ymax"),
    ):
        boundary_centroids = boundary_centroids.withColumn(
            coordinate, F.expr(f"{function_name}(_boundary_geom)")
        )
    event_candidates = (
        event_points.crossJoin(boundary_centroids)
        .filter(
            (F.col("longitude") >= F.col("_xmin"))
            & (F.col("longitude") <= F.col("_xmax"))
            & (F.col("latitude") >= F.col("_ymin"))
            & (F.col("latitude") <= F.col("_ymax"))
        )
        .filter(
            F.expr(
                "st_covers(_boundary_geom, try_to_geometry(_event_geojson))"
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
    risk_tertiles = risk.filter(F.col("risk_score").isNotNull()).withColumn(
        "risk_tertile", F.ntile(3).over(risk_window)
    ).select("psgc_code", "risk_tertile")
    risk = (
        risk.join(risk_tertiles, "psgc_code", "left")
        .withColumn(
            "risk_level",
            F.when(F.col("risk_tertile") == 3, "High")
            .when(F.col("risk_tertile") == 2, "Medium")
            .when(F.col("risk_tertile") == 1, "Low"),
        )
        .withColumn("risk_window_start", F.add_months(F.current_date(), -60))
        .withColumn("risk_window_end", F.current_date())
        .withColumn("population_year", F.lit(latest_population_year).cast("int"))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("calculated_at", F.lit(calculated_at).cast("timestamp"))
    )
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
        F.col("psgc_code").isNotNull()
    )
    budget = budget.groupBy("psgc_code", "fiscal_year").agg(
        F.sum("total_appropriation").alias("total_appropriation"),
        F.sum("total_expenditure").alias("total_expenditure"),
    ).withColumn(
        "ldrrmf_utilization_score",
        F.when(
            F.col("total_appropriation") != 0,
            F.col("total_expenditure") / F.col("total_appropriation") * 100,
        ).cast("decimal(7,2)"),
    ).filter(F.col("ldrrmf_utilization_score").isNotNull())
    latest_fiscal = budget.groupBy("psgc_code").agg(F.max("fiscal_year").alias("fiscal_year"))
    budget = budget.join(latest_fiscal, ["psgc_code", "fiscal_year"], "inner").select(
        "psgc_code",
        "fiscal_year",
        "ldrrmf_utilization_score",
    )
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
    prep_tertiles = preparedness.filter(F.col("preparedness_score").isNotNull()).withColumn(
        "preparedness_tertile", F.ntile(3).over(prep_window)
    ).select("psgc_code", "preparedness_tertile")
    preparedness = preparedness.join(prep_tertiles, "psgc_code", "left").withColumn(
        "preparedness_gap_rank",
        F.when(
            F.col("preparedness_gap").isNotNull(),
            F.rank().over(Window.orderBy(F.col("preparedness_gap").desc(), F.col("psgc_code"))),
        ),
    ).withColumn("run_id", F.lit(run_id)).withColumn(
        "calculated_at", F.lit(calculated_at).cast("timestamp")
    )
    vulnerability = preparedness.withColumn(
        "priority_category",
        F.when((F.col("risk_tertile") == 3) & (F.col("preparedness_tertile") == 1), "Highest priority")
        .when((F.col("risk_tertile") == 3) & (F.col("preparedness_tertile") > 1), "Monitor")
        .when((F.col("risk_tertile") < 3) & (F.col("preparedness_tertile") == 1), "Capacity concern")
        .when(F.col("risk_tertile").isNotNull() & F.col("preparedness_tertile").isNotNull(), "Lower priority"),
    )
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
    ).withColumn("run_id", F.lit(run_id)).withColumn(
        "calculated_at", F.lit(calculated_at).cast("timestamp")
    )
    counts = {
        "active_lgus": lgu.count(),
        "population_scores": population_score.count(),
        "lgu_with_spatial_events": activity.count(),
        "risk_scores": risk.filter(F.col("risk_score").isNotNull()).count(),
        "preparedness_scores": preparedness.filter(F.col("preparedness_score").isNotNull()).count(),
        "highest_priority_lgus": priority_lgus.count(),
    }
    checked_at = calculated_at
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
    output_specs = (
        ("risk_level_by_lgu", risk, ("psgc_code",), "risk_level", ("Low", "Medium", "High")),
        ("preparedness_gap_by_lgu", preparedness, ("psgc_code",), None, ()),
        ("vulnerability_priority_by_lgu", vulnerability, ("psgc_code",), "priority_category", ("Highest priority", "Monitor", "Capacity concern", "Lower priority")),
        ("cmci_preparedness_priority_by_indicator", priority_indicators, ("cmci_year", "indicator_code"), None, ()),
    )
    validation_failures = []
    for output_name, frame, key_columns, category_column, allowed_categories in output_specs:
        target = table("platinum", output_name)
        row_count = frame.count()
        duplicate_groups = (
            frame.groupBy(*key_columns)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        wrong_run_count = frame.filter(
            F.col("run_id").isNull() | (F.col("run_id") != run_id)
        ).count()
        for rule, failed_count, details in (
            ("unique_output_grain", duplicate_groups, {"key_columns": key_columns}),
            ("current_run_id", wrong_run_count, {"expected_run_id": run_id}),
        ):
            status = "FAIL" if failed_count else "PASS"
            if failed_count:
                validation_failures.append(f"{target}:{rule}")
            results.append((run_id, checked_at, "platinum", target, rule, "BLOCKING", status,
                row_count, failed_count, json.dumps(details, sort_keys=True)))
        if category_column:
            invalid_categories = frame.filter(
                F.col(category_column).isNotNull()
                & ~F.col(category_column).isin(*allowed_categories)
            ).count()
            status = "FAIL" if invalid_categories else "PASS"
            if invalid_categories:
                validation_failures.append(f"{target}:{category_column}_allowed_values")
            results.append((run_id, checked_at, "platinum", target,
                f"{category_column}_allowed_values", "BLOCKING", status, row_count,
                invalid_categories, json.dumps({"allowed": allowed_categories}, sort_keys=True)))
        metric_error = None
        if output_name == "risk_level_by_lgu":
            scores_missing = F.col("population_exposure_score").isNull() | F.col(
                "earthquake_activity_score"
            ).isNull()
            metric_error = (
                F.col("risk_score").isNull() != scores_missing
            ) | (
                ~scores_missing
                & (
                    F.abs(
                        F.col("risk_score")
                        - F.col("population_exposure_score")
                        * F.col("earthquake_activity_score")
                    )
                    > F.lit(0.01)
                )
            )
            expected_risk_level = (
                F.when(F.col("risk_tertile") == 3, "High")
                .when(F.col("risk_tertile") == 2, "Medium")
                .when(F.col("risk_tertile") == 1, "Low")
            )
            metric_error = metric_error | (
                F.col("risk_level").isNull() != expected_risk_level.isNull()
            ) | (
                F.col("risk_level").isNotNull()
                & (F.col("risk_level") != expected_risk_level)
            )
        elif output_name in ("preparedness_gap_by_lgu", "vulnerability_priority_by_lgu"):
            inputs_missing = F.col("cmci_capacity_score").isNull() | F.col(
                "ldrrmf_utilization_score"
            ).isNull()
            missing_gap_input = inputs_missing | F.col("risk_score").isNull()
            metric_error = (
                F.col("preparedness_score").isNull() != inputs_missing
            ) | (
                ~inputs_missing
                & (
                    F.abs(
                        F.col("preparedness_score")
                        - F.col("cmci_capacity_score")
                        - F.col("ldrrmf_utilization_score")
                    )
                    > F.lit(0.01)
                )
            ) | (F.col("preparedness_gap").isNull() != missing_gap_input) | (
                ~missing_gap_input
                & (
                    F.abs(
                        F.col("preparedness_gap")
                        - F.col("preparedness_score")
                        + F.col("risk_score")
                    )
                    > F.lit(0.01)
                )
            )
            if output_name == "vulnerability_priority_by_lgu":
                category_missing = F.col("priority_category").isNull()
                tertile_missing = F.col("risk_tertile").isNull() | F.col(
                    "preparedness_tertile"
                ).isNull()
                metric_error = metric_error | (
                    category_missing != tertile_missing
                )
                expected_category = (
                    F.when(
                        (F.col("risk_tertile") == 3)
                        & (F.col("preparedness_tertile") == 1),
                        "Highest priority",
                    )
                    .when(
                        (F.col("risk_tertile") == 3)
                        & (F.col("preparedness_tertile") > 1),
                        "Monitor",
                    )
                    .when(
                        (F.col("risk_tertile") < 3)
                        & (F.col("preparedness_tertile") == 1),
                        "Capacity concern",
                    )
                    .when(
                        F.col("risk_tertile").isNotNull()
                        & F.col("preparedness_tertile").isNotNull(),
                        "Lower priority",
                    )
                )
                metric_error = metric_error | (
                    F.col("priority_category").isNotNull()
                    & (F.col("priority_category") != expected_category)
                )
        elif output_name == "cmci_preparedness_priority_by_indicator":
            metric_error = (
                F.col("mean_indicator_percentile").isNull()
                | (F.col("mean_indicator_percentile") < 0)
                | (F.col("mean_indicator_percentile") > 100)
                | F.col("priority_lgu_count").isNull()
                | (F.col("priority_lgu_count") <= 0)
            )
        if metric_error is not None:
            invalid_metrics = frame.filter(metric_error).count()
            status = "FAIL" if invalid_metrics else "PASS"
            if invalid_metrics:
                validation_failures.append(f"{target}:metric_consistency")
            results.append((run_id, checked_at, "platinum", target,
                "metric_formula_null_and_range_consistency", "BLOCKING", status,
                row_count, invalid_metrics, "{}"))
    spark.createDataFrame(results, schema).write.mode("append").saveAsTable(table("monitoring", "dq_result"))
    if validation_failures:
        raise RuntimeError("Blocking Platinum output quality failures: " + ", ".join(validation_failures))
    # Delay replacement until every output transform and its coverage actions succeed.
    _write(risk, table("platinum", "risk_level_by_lgu"))
    _write(preparedness, table("platinum", "preparedness_gap_by_lgu"))
    _write(vulnerability, table("platinum", "vulnerability_priority_by_lgu"))
    _write(priority_indicators, table("platinum", "cmci_preparedness_priority_by_indicator"))
    print(f"Experimental Platinum v1 complete: {json.dumps(counts, sort_keys=True)}")


if __name__ == "__main__":
    main()
