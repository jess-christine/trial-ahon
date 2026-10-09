"""Build the Gold dimensions and source facts supported by reviewed inputs."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

CMCI_CODE_DIR = Path(__file__).resolve().parents[1] / "01_bronze_ingest"
sys.path.insert(0, str(CMCI_CODE_DIR))

from cmci_common import INDICATORS_BY_PILLAR

CATALOG = os.environ.get("AHON_CATALOG", "ahon")


def _table(name: str) -> str:
    return name.replace("ahon.", f"{CATALOG}.", 1)


LGU_MASTER = _table("ahon.reference.lgu_master")
PSGC_SILVER = _table("ahon.silver.psgc_clean")
BLGF_SILVER = _table("ahon.silver.blgf_ldrrmf_annual_lgu_clean")
PHIVOLCS_SILVER = _table("ahon.silver.philvolcs_earthquake_data_clean")
CMCI_SILVER_PREFIX = _table("ahon.silver.cmci_")


def indicator_records() -> list[tuple[str, str, str]]:
    """Return the approved CMCI indicator dimension in stable code order."""
    records = [
        (code, pillar, name)
        for pillar, indicators in INDICATORS_BY_PILLAR.items()
        for name, code in indicators.items()
    ]
    codes = [record[0] for record in records]
    if len(codes) != len(set(codes)):
        raise ValueError("CMCI indicator configuration contains duplicate codes")
    if any(not code or len(code) > 10 for code in codes):
        raise ValueError("CMCI indicator codes must fit the Gold key contract")
    return sorted(records)


def assert_unique(frame: DataFrame, keys: tuple[str, ...], label: str) -> None:
    from pyspark.sql import functions as F

    duplicates = (
        frame.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicates:
        raise ValueError(f"{label} contains duplicate key values for {keys}")


def replace_snapshot(spark: SparkSession, table_name: str, frame: DataFrame) -> None:
    """Atomically replace a small deterministic snapshot after schema checks."""
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(
            f"Gold table {table_name} is missing; run 07_gold_setup.sql first"
        )
    existing = spark.table(table_name).schema
    expected = frame.schema
    if existing.fieldNames() != expected.fieldNames() or [
        field.dataType for field in existing.fields
    ] != [field.dataType for field in expected.fields]:
        raise ValueError(f"Gold schema drift for {table_name}")
    frame.write.format("delta").mode("overwrite").insertInto(table_name)


def build_dim_lgu(spark: SparkSession) -> None:
    """Use active reviewed LGUs; enrich hierarchy only by exact PSGC code."""
    from pyspark.sql import functions as F

    reference = spark.table(LGU_MASTER).filter(F.col("is_active") == F.lit(True))
    assert_unique(reference, ("psgc_code",), "active lgu_master")

    code_columns = (
        "region_code",
        "province_code",
        "municipality_code",
        "barangay_code",
    )
    psgc = spark.table(PSGC_SILVER).select("psgc_code", *code_columns)
    relevant = psgc.join(reference.select("psgc_code"), "psgc_code", "inner")
    hierarchy = relevant.groupBy("psgc_code").agg(
        F.countDistinct(F.struct(*code_columns)).alias("hierarchy_versions"),
        *[
            F.first(F.col(column), ignorenulls=False).alias(column)
            for column in code_columns
        ],
    )
    conflict_count = hierarchy.filter(
        F.col("hierarchy_versions") > 1
    ).count()

    joined = reference.alias("reference").join(
        hierarchy.alias("hierarchy"), "psgc_code", "left"
    )
    missing_hierarchy = joined.filter(
        F.col("hierarchy.region_code").isNull()
        | F.col("hierarchy.municipality_code").isNull()
    ).count()
    province_conflicts = joined.filter(
        F.col("reference.province_code").isNotNull()
        & F.col("hierarchy.province_code").isNotNull()
        & (F.col("reference.province_code") != F.col("hierarchy.province_code"))
    ).count()
    frame = joined.select(
        F.col("psgc_code").cast("string").alias("psgc_code"),
        F.col("reference.lgu_name").cast("string").alias("area_name"),
        F.col("reference.geographic_level")
        .cast("string")
        .alias("geographic_level"),
        F.when(
            F.col("hierarchy.hierarchy_versions") == 1,
            F.col("hierarchy.region_code"),
        )
        .cast("string")
        .alias("region_code"),
        F.coalesce(
            F.col("reference.province_code"),
            F.when(
                F.col("hierarchy.hierarchy_versions") == 1,
                F.col("hierarchy.province_code"),
            ),
        )
        .cast("string")
        .alias("province_code"),
        F.when(
            F.col("hierarchy.hierarchy_versions") == 1,
            F.col("hierarchy.municipality_code"),
        )
        .cast("string")
        .alias("municipality_code"),
        F.when(
            F.col("hierarchy.hierarchy_versions") == 1,
            F.col("hierarchy.barangay_code"),
        )
        .cast("string")
        .alias("barangay_code"),
    )
    assert_unique(frame, ("psgc_code",), "dim_lgu source")
    replace_snapshot(spark, _table("ahon.gold.dim_lgu"), frame)
    print(
        f"{_table('ahon.gold.dim_lgu')}: "
        f"{frame.count()} active LGUs; "
        f"{missing_hierarchy} have missing hierarchy codes, "
        f"{conflict_count} have conflicting PSGC hierarchy versions, "
        f"{province_conflicts} disagree with the active reference province code"
    )


def build_dim_cmci_indicator(spark: SparkSession) -> None:
    from pyspark.sql.types import StringType, StructField, StructType

    records = indicator_records()
    schema = StructType(
        [
            StructField("indicator_code", StringType(), False),
            StructField("pillar_name", StringType(), False),
            StructField("indicator_name", StringType(), False),
        ]
    )
    frame = spark.createDataFrame(records, schema)
    assert_unique(frame, ("indicator_code",), "CMCI indicator configuration")
    replace_snapshot(spark, _table("ahon.gold.dim_cmci_indicator"), frame)
    print(f"{_table('ahon.gold.dim_cmci_indicator')}: {len(records)} approved indicators")


def build_fact_cmci_indicator(spark: SparkSession) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType, IntegerType

    dimension = spark.table(_table("ahon.gold.dim_lgu")).select(
        "psgc_code", "geographic_level"
    )
    indicators = spark.table(_table("ahon.gold.dim_cmci_indicator")).select(
        "indicator_code"
    )
    frames = []
    for pillar, mapping in INDICATORS_BY_PILLAR.items():
        table_name = CMCI_SILVER_PREFIX + pillar.lower().replace(" ", "_")
        source = spark.table(table_name)
        for indicator_code in mapping.values():
            frames.append(
                source.select(
                    F.col("psgc_code").cast("string").alias("psgc_code"),
                    F.col("year").cast(IntegerType()).alias("year"),
                    F.lit(indicator_code).alias("indicator_code"),
                    F.col("lgu").cast("string").alias("psgc_name"),
                    F.col("cmci_name").cast("string").alias("cmci_name"),
                    F.col(indicator_code)
                    .cast(DecimalType(18, 6))
                    .alias("raw_score"),
                )
            )

    if not frames:
        raise ValueError("No configured CMCI Silver indicators were found")
    long_form = frames[0]
    for frame in frames[1:]:
        long_form = long_form.unionByName(frame)
    fact = long_form.join(dimension, "psgc_code", "left").select(
        "psgc_code",
        "year",
        "indicator_code",
        "psgc_name",
        "cmci_name",
        "geographic_level",
        "raw_score",
    )
    if fact.filter(
        F.col("psgc_code").isNull()
        | F.col("year").isNull()
        | F.col("geographic_level").isNull()
    ).limit(1).count():
        raise ValueError("CMCI Gold rows have missing keys or LGU dimension matches")
    unknown_indicators = fact.select("indicator_code").distinct().join(
        indicators, "indicator_code", "left_anti"
    )
    if unknown_indicators.limit(1).count():
        raise ValueError("CMCI Gold contains codes absent from dim_cmci_indicator")
    assert_unique(
        fact,
        ("psgc_code", "year", "indicator_code"),
        "fact_cmci_indicator",
    )
    # Prevent an empty upstream refresh from replacing an existing good fact.
    if not fact.limit(1).count():
        raise ValueError("CMCI Silver is empty; fact_cmci_indicator was not replaced")
    replace_snapshot(spark, _table("ahon.gold.fact_cmci_indicator"), fact)
    print(f"{_table('ahon.gold.fact_cmci_indicator')}: {fact.count()} LGU-year-indicator rows")


def build_fact_population(spark: SparkSession) -> None:
    """Load PSA's city/municipality grain without allocating other levels."""
    from pyspark.sql import functions as F

    population = spark.table(_table("ahon.silver.psa_population_clean")).filter(
        F.col("geographic_level") == "City/Municipality"
    )
    invalid_key_count = population.filter(
        F.col("geographic_location").isNull() | F.col("census_year").isNull()
    ).count()
    if invalid_key_count:
        raise ValueError(
            "PSA city/municipality rows have "
            f"{invalid_key_count} null geographic path/year keys"
        )
    assert_unique(
        population,
        ("geographic_location", "census_year"),
        "PSA city/municipality Silver source",
    )
    reference = (
        spark.table(LGU_MASTER)
        .filter(F.col("is_active") == F.lit(True))
        .filter(F.col("province_name").isNotNull())
        .select(
            F.col("lgu_name").alias("reference_lgu_name"),
            F.col("province_name").alias("reference_province_name"),
            F.col("psgc_code").alias("candidate_psgc_code"),
        )
    )
    candidates = (
        population.alias("population")
        .join(
            reference.alias("reference"),
            (F.col("population.lgu_name") == F.col("reference.reference_lgu_name"))
            & (
                F.col("population.province_name")
                == F.col("reference.reference_province_name")
            ),
            "left",
        )
        .groupBy(
            F.col("population.geographic_location"),
            F.col("population.census_year"),
            F.col("population.total_population"),
            F.col("population.urban_population"),
            F.col("population.percent_urban"),
        )
        .agg(
            F.countDistinct("candidate_psgc_code").alias("candidate_count"),
            F.first("candidate_psgc_code").alias("candidate_psgc_code"),
        )
    )
    fact = candidates.select(
        "geographic_location",
        F.when(F.col("candidate_count") == 1, F.col("candidate_psgc_code"))
        .cast("string")
        .alias("psgc_code"),
        F.col("census_year").cast("int").alias("year"),
        "total_population",
        "urban_population",
        "percent_urban",
    )
    assert_unique(
        fact, ("geographic_location", "year"), "fact_population source"
    )
    replace_snapshot(spark, _table("ahon.gold.fact_population"), fact)
    unmatched = fact.filter(F.col("psgc_code").isNull()).count()
    print(
        f"{_table('ahon.gold.fact_population')}: {fact.count()} city/municipality rows; "
        f"{unmatched} unmatched or ambiguous exact LGU/province names remain null"
    )


def build_fact_ldrrmf(spark: SparkSession) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType, IntegerType, LongType

    all_source = spark.table(BLGF_SILVER)
    source = all_source.filter(
        F.col("lgu_type").isin("City", "Municipality")
    )
    excluded_by_type = {
        row["lgu_type"] or "<NULL>": row["count"]
        for row in all_source.filter(
            ~F.col("lgu_type").isin("City", "Municipality")
            | F.col("lgu_type").isNull()
        ).groupBy("lgu_type").count().collect()
    }
    reference = (
        spark.table(LGU_MASTER)
        .filter(F.col("is_active") == F.lit(True))
        .filter(F.col("geographic_level").isin("City", "Mun", "Municipality", "City/Municipality"))
        .select(
            F.col("lgu_name").alias("reference_lgu_name"),
            F.col("province_name").alias("reference_province_name"),
            F.col("psgc_code").alias("candidate_psgc_code"),
        )
    )
    source_columns = source.columns
    candidates = (
        source.alias("source")
        .join(
            reference.alias("reference"),
            (F.col("source.lgu_name") == F.col("reference.reference_lgu_name"))
            & (F.col("source.province") == F.col("reference.reference_province_name")),
            "left",
        )
        .groupBy(*[F.col(f"source.{column}") for column in source_columns])
        .agg(
            F.countDistinct("candidate_psgc_code").alias("candidate_count"),
            F.first("candidate_psgc_code").alias("candidate_psgc_code"),
        )
    )
    fact = candidates.select(
        F.xxhash64("_source_ref", "_row_hash").cast(LongType()).alias(
            "ldrrmf_fact_key"
        ),
        F.when(F.col("candidate_count") == 1, F.col("candidate_psgc_code"))
        .cast("string")
        .alias("psgc_code"),
        F.col("fiscal_year").cast(IntegerType()).alias("fiscal_year"),
        "region",
        "province",
        "lgu_name",
        "lgu_type",
        F.col("70pct_ldrrmf_budget_appropriation")
        .cast(DecimalType(18, 2))
        .alias("appropriation_70_pct"),
        F.col("70pct_ldrrmf_expenditures")
        .cast(DecimalType(18, 2))
        .alias("expenditure_70_pct"),
        F.col("30pct_quick_response_fund_budget_appropriation")
        .cast(DecimalType(18, 2))
        .alias("appropriation_30_pct"),
        F.col("30pct_quick_response_fund_expenditures")
        .cast(DecimalType(18, 2))
        .alias("expenditure_30_pct"),
        F.col("total_budget_appropriation")
        .cast(DecimalType(18, 2))
        .alias("total_appropriation"),
        F.col("total_expenditures")
        .cast(DecimalType(18, 2))
        .alias("total_expenditure"),
        F.when(
            F.col("total_budget_appropriation") != 0,
            F.col("total_expenditures") * F.lit(100)
            / F.col("total_budget_appropriation"),
        )
        .cast(DecimalType(7, 2))
        .alias("utilization_rate"),
        F.when(F.col("candidate_count") == 1, F.lit("MATCHED"))
        .when(F.col("candidate_count") > 1, F.lit("AMBIGUOUS"))
        .otherwise(F.lit("UNMATCHED"))
        .alias("match_status"),
        F.lit(None).cast(DecimalType(5, 4)).alias("match_confidence"),
    )
    assert_unique(fact, ("ldrrmf_fact_key",), "fact_ldrrmf source")
    replace_snapshot(spark, _table("ahon.gold.fact_ldrrmf"), fact)
    print(
        f"{_table('ahon.gold.fact_ldrrmf')}: {fact.count()} city/municipality rows; "
        f"{fact.filter(F.col('match_status') == 'MATCHED').count()} exact matches, "
        f"{fact.filter(F.col('match_status') != 'MATCHED').count()} unmatched/ambiguous; "
        f"excluded types={excluded_by_type}; "
        "all excluded rows remain in Silver; "
        "utilization_rate is total expenditure / total appropriation * 100; zero appropriation stays NULL"
    )


def build_fact_earthquake_event(spark: SparkSession) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType, LongType

<<<<<<< Updated upstream
    source = spark.table(PHIVOLCS_SILVER).dropDuplicates(["id"])
=======
    source = spark.table(PHIVOLCS_SILVER)
    assert_unique(source, ("id",), "PHIVOLCS Silver event source")
>>>>>>> Stashed changes
    boundary = (
        spark.table(
            _table("ahon.silver.geoportal_city_municipality_boundary_clean")
        )
        .join(
            spark.table(_table("ahon.gold.dim_lgu"))
            .filter(F.col("geographic_level").isin("City", "Mun", "Municipality", "City/Municipality"))
            .select("psgc_code"),
            "psgc_code",
            "inner",
        )
        .select("psgc_code", "boundary_wkb_hex")
<<<<<<< Updated upstream
        .withColumn("_boundary_geom", F.expr("try_to_geometry(unhex(boundary_wkb_hex))"))
=======
        .withColumn("_boundary_geom", F.expr("st_setsrid(try_to_geometry(unhex(boundary_wkb_hex)), 4326)"))
>>>>>>> Stashed changes
        .withColumn("_xmin", F.expr("st_xmin(_boundary_geom)"))
        .withColumn("_xmax", F.expr("st_xmax(_boundary_geom)"))
        .withColumn("_ymin", F.expr("st_ymin(_boundary_geom)"))
        .withColumn("_ymax", F.expr("st_ymax(_boundary_geom)"))
    )
    event_json = F.concat(
        F.lit('{"type":"Point","coordinates":['),
        F.col("longitude").cast("string"), F.lit(","),
        F.col("latitude").cast("string"), F.lit("]}"),
    )
    candidates = (
        source.select("id", "longitude", "latitude")
        .withColumn("_event_geojson", event_json)
        .withColumn("_event_geom", F.expr("try_to_geometry(_event_geojson)"))
        .crossJoin(boundary)
        .filter(
            (F.col("longitude") >= F.col("_xmin"))
            & (F.col("longitude") <= F.col("_xmax"))
            & (F.col("latitude") >= F.col("_ymin"))
            & (F.col("latitude") <= F.col("_ymax"))
        )
        .filter(F.expr("st_covers(_boundary_geom, _event_geom)"))
        .select("id", "psgc_code")
        .groupBy("id")
        .agg(
            F.countDistinct("psgc_code").alias("match_count"),
            F.first("psgc_code").alias("candidate_psgc_code"),
        )
    )
    mapped = source.join(candidates, "id", "left")
    fact = mapped.select(
        F.col("id").cast(LongType()).alias("earthquake_fact_key"),
        F.when(F.col("match_count") == 1, F.col("candidate_psgc_code"))
        .cast("string")
        .alias("psgc_code"),
        F.col("location_description").cast("string").alias("location"),
        F.col("event_time").cast("timestamp").alias("timestamp"),
        F.col("depth").cast(DecimalType(10, 2)).alias("depth"),
        F.col("magnitude").cast(DecimalType(4, 2)).alias("magnitude"),
        F.col("longitude").cast(DecimalType(10, 7)).alias("longitude"),
        F.col("latitude").cast(DecimalType(10, 7)).alias("latitude"),
        F.when(F.col("match_count") == 1, F.lit("MATCHED"))
        .when(F.col("match_count") > 1, F.lit("AMBIGUOUS"))
        .otherwise(F.lit("UNMATCHED"))
        .alias("match_status"),
        F.lit(None).cast(DecimalType(5, 4)).alias("match_confidence"),
    )
    assert_unique(fact, ("earthquake_fact_key",), "fact_earthquake_event source")
    replace_snapshot(spark, _table("ahon.gold.fact_earthquake_event"), fact)
    print(
        f"{_table('ahon.gold.fact_earthquake_event')}: {fact.count()} valid events; "
        f"{fact.filter(F.col('match_status') == 'MATCHED').count()} spatially matched, "
        f"{fact.filter(F.col('match_status') != 'MATCHED').count()} unmatched/ambiguous"
    )


def main() -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
    build_dim_lgu(spark)
    build_dim_cmci_indicator(spark)
    build_fact_cmci_indicator(spark)
    build_fact_population(spark)
    build_fact_ldrrmf(spark)
    build_fact_earthquake_event(spark)


if __name__ == "__main__":
    main()
