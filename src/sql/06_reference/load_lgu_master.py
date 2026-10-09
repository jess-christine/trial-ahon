import os

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.getOrCreate()

# =====================================================
# CONFIG
# =====================================================

CATALOG = os.environ.get("AHON_CATALOG", "ahon")
SOURCE_VOLUME = os.environ.get("AHON_SOURCE_VOLUME", f"/Volumes/{CATALOG}/reference/source")
SOURCE_PATH = f"{SOURCE_VOLUME}/PSGC-2Q-2026-Publication-Datafile.xlsx"

SOURCE_FILE = "PSGC-2Q-2026-Publication-Datafile.xlsx"
SOURCE_SHEET = "PSGC"
SOURCE_PUBLICATION_DATE = "2026-06-30"

TARGET_TABLE = f"{CATALOG}.reference.lgu_master"

EXPECTED_CITY_COUNT = 149
EXPECTED_MUNICIPALITY_COUNT = 1493
EXPECTED_TOTAL_COUNT = 1642
EXPECTED_PROVINCE_LINKED_COUNT = 1599
EXPECTED_NO_PROVINCE_COUNT = 43


# =====================================================
# SOURCE COLUMN NAMES
# =====================================================

PSGC_CODE_COLUMN = "10-digit PSGC"
NAME_COLUMN = "Name"
CORRESPONDENCE_CODE_COLUMN = "Correspondence Code"
GEOGRAPHIC_LEVEL_COLUMN = "Geographic Level"
OLD_NAME_COLUMN = "Old names"
CITY_CLASS_COLUMN = "City Class"

INCOME_CLASSIFICATION_COLUMN = (
    "Income\nClassification "
    "(DOF DO No. 074.2024)"
)

POPULATION_COLUMN = "2024 Population"

REQUIRED_SOURCE_COLUMNS = {
    PSGC_CODE_COLUMN,
    NAME_COLUMN,
    CORRESPONDENCE_CODE_COLUMN,
    GEOGRAPHIC_LEVEL_COLUMN,
    OLD_NAME_COLUMN,
    CITY_CLASS_COLUMN,
    INCOME_CLASSIFICATION_COLUMN,
    POPULATION_COLUMN,
}


# =====================================================
# SPARK SOURCE SCHEMA
# =====================================================

LGU_SOURCE_SCHEMA = StructType(
    [
        StructField(
            "psgc_code",
            StringType(),
            nullable=False,
        ),
        StructField(
            "correspondence_code",
            StringType(),
            nullable=True,
        ),
        StructField(
            "lgu_name",
            StringType(),
            nullable=False,
        ),
        StructField(
            "province_code",
            StringType(),
            nullable=True,
        ),
        StructField(
            "province_name",
            StringType(),
            nullable=True,
        ),
        StructField(
            "geographic_level",
            StringType(),
            nullable=False,
        ),
        StructField(
            "old_name",
            StringType(),
            nullable=True,
        ),
        StructField(
            "city_class",
            StringType(),
            nullable=True,
        ),
        StructField(
            "income_classification",
            StringType(),
            nullable=True,
        ),
        StructField(
            "population_2024",
            LongType(),
            nullable=True,
        ),
    ]
)


# =====================================================
# SPARK CONFIG
# =====================================================

spark.conf.set(
    "spark.sql.session.timeZone",
    "UTC",
)

print("PSGC LGU master loader started")
print("Source: " + SOURCE_PATH)
print("Target: " + TARGET_TABLE)


# =====================================================
# VALIDATE TARGET TABLE
# =====================================================

if not spark.catalog.tableExists(TARGET_TABLE):
    raise RuntimeError(
        "Target table does not exist: "
        + TARGET_TABLE
    )


target_df = spark.table(
    TARGET_TABLE
)

required_target_columns = {
    "psgc_code",
    "correspondence_code",
    "lgu_name",
    "province_code",
    "province_name",
    "geographic_level",
    "old_name",
    "city_class",
    "income_classification",
    "population_2024",
    "source_file",
    "source_publication_date",
    "is_active",
    "created_timestamp",
    "updated_timestamp",
}

actual_target_columns = set(
    target_df.columns
)

missing_target_columns = (
    required_target_columns
    - actual_target_columns
)

if missing_target_columns:
    raise RuntimeError(
        "Target table is missing required columns: "
        + str(sorted(missing_target_columns))
    )


# =====================================================
# READ PSGC WORKBOOK
# =====================================================

try:
    psgc_pdf = pd.read_excel(
        SOURCE_PATH,
        sheet_name=SOURCE_SHEET,
        dtype={
            PSGC_CODE_COLUMN: "string",
            CORRESPONDENCE_CODE_COLUMN: "string",
        },
        engine="openpyxl",
    )

except FileNotFoundError as error:
    raise RuntimeError(
        "PSGC workbook was not found: "
        + SOURCE_PATH
    ) from error

except ValueError as error:
    raise RuntimeError(
        "Unable to read sheet "
        + SOURCE_SHEET
        + " from the PSGC workbook: "
        + str(error)
    ) from error

except Exception as error:
    raise RuntimeError(
        "Unable to read the PSGC workbook: "
        + str(error)
    ) from error


# =====================================================
# VALIDATE SOURCE COLUMNS
# =====================================================

actual_source_columns = set(
    psgc_pdf.columns
)

missing_source_columns = (
    REQUIRED_SOURCE_COLUMNS
    - actual_source_columns
)

if missing_source_columns:
    raise RuntimeError(
        "PSGC workbook is missing required columns: "
        + str(sorted(missing_source_columns))
    )

print("PSGC sheet loaded successfully")

print(
    "Source rows read: "
    + str(len(psgc_pdf))
)

# =====================================================
# BUILD PROVINCE LOOKUP
# =====================================================

province_pdf = (
    psgc_pdf[
        psgc_pdf[
            GEOGRAPHIC_LEVEL_COLUMN
        ].eq(
            "Prov"
        )
    ][
        [
            PSGC_CODE_COLUMN,
            NAME_COLUMN,
        ]
    ]
    .copy()
)

province_pdf["province_code"] = (
    province_pdf[
        PSGC_CODE_COLUMN
    ]
    .astype("string")
    .str.strip()
)

province_pdf["province_name"] = (
    province_pdf[
        NAME_COLUMN
    ]
    .astype("string")
    .str.strip()
)

province_pdf["province_prefix"] = (
    province_pdf[
        "province_code"
    ]
    .str.slice(
        0,
        5,
    )
)

duplicate_province_prefix_count = int(
    province_pdf[
        "province_prefix"
    ]
    .duplicated()
    .sum()
)

if duplicate_province_prefix_count > 0:
    raise RuntimeError(
        "Province lookup contains "
        + str(duplicate_province_prefix_count)
        + " duplicate province prefixes"
    )

print(
    "Province rows extracted: "
    + str(len(province_pdf))
)

# =====================================================
# FILTER CITIES AND MUNICIPALITIES
# =====================================================

lgu_pdf = (
    psgc_pdf[
        psgc_pdf[
            GEOGRAPHIC_LEVEL_COLUMN
        ].isin(
            [
                "City",
                "Mun",
            ]
        )
    ]
    .copy()
)

lgu_pdf["province_prefix"] = (
    lgu_pdf[
        PSGC_CODE_COLUMN
    ]
    .astype("string")
    .str.strip()
    .str.slice(
        0,
        5,
    )
)

lgu_pdf = (
    lgu_pdf
    .merge(
        province_pdf[
            [
                "province_prefix",
                "province_code",
                "province_name",
            ]
        ],
        on="province_prefix",
        how="left",
        validate="many_to_one",
    )
    .drop(
        columns=[
            "province_prefix",
        ]
    )
)

preview_city_count = int(
    lgu_pdf[
        GEOGRAPHIC_LEVEL_COLUMN
    ]
    .eq("City")
    .sum()
)

preview_municipality_count = int(
    lgu_pdf[
        GEOGRAPHIC_LEVEL_COLUMN
    ]
    .eq("Mun")
    .sum()
)

preview_total_count = len(lgu_pdf)

print(
    "Cities extracted: "
    + str(preview_city_count)
)

print(
    "Municipalities extracted: "
    + str(preview_municipality_count)
)

print(
    "Total LGUs extracted: "
    + str(preview_total_count)
)


# =====================================================
# VALIDATE EXTRACTED COUNTS
# =====================================================

if preview_city_count != EXPECTED_CITY_COUNT:
    raise RuntimeError(
        "Expected "
        + str(EXPECTED_CITY_COUNT)
        + " cities, found "
        + str(preview_city_count)
    )

if (
    preview_municipality_count
    != EXPECTED_MUNICIPALITY_COUNT
):
    raise RuntimeError(
        "Expected "
        + str(EXPECTED_MUNICIPALITY_COUNT)
        + " municipalities, found "
        + str(preview_municipality_count)
    )

if preview_total_count != EXPECTED_TOTAL_COUNT:
    raise RuntimeError(
        "Expected "
        + str(EXPECTED_TOTAL_COUNT)
        + " total LGUs, found "
        + str(preview_total_count)
    )

# =====================================================
# VALIDATE PROVINCE DERIVATION
# =====================================================

province_linked_count = int(
    lgu_pdf[
        "province_code"
    ]
    .notna()
    .sum()
)

no_province_count = int(
    lgu_pdf[
        "province_code"
    ]
    .isna()
    .sum()
)

if (
    province_linked_count
    != EXPECTED_PROVINCE_LINKED_COUNT
):
    raise RuntimeError(
        "Expected "
        + str(EXPECTED_PROVINCE_LINKED_COUNT)
        + " province-linked LGUs, found "
        + str(province_linked_count)
    )

if no_province_count != EXPECTED_NO_PROVINCE_COUNT:
    raise RuntimeError(
        "Expected "
        + str(EXPECTED_NO_PROVINCE_COUNT)
        + " LGUs without a province, found "
        + str(no_province_count)
    )

invalid_province_pair_count = int(
    (
        lgu_pdf["province_code"].isna()
        != lgu_pdf["province_name"].isna()
    ).sum()
)

if invalid_province_pair_count > 0:
    raise RuntimeError(
        "Found "
        + str(invalid_province_pair_count)
        + " LGUs with incomplete province pairs"
    )

print(
    "Province-linked LGUs: "
    + str(province_linked_count)
)

print(
    "LGUs without province assignment: "
    + str(no_province_count)
)

# =====================================================
# RENAME AND SELECT SOURCE COLUMNS
# =====================================================

clean_pdf = lgu_pdf.rename(
    columns={
        PSGC_CODE_COLUMN: "psgc_code",
        CORRESPONDENCE_CODE_COLUMN: (
            "correspondence_code"
        ),
        NAME_COLUMN: "lgu_name",
        GEOGRAPHIC_LEVEL_COLUMN: (
            "geographic_level"
        ),
        OLD_NAME_COLUMN: "old_name",
        CITY_CLASS_COLUMN: "city_class",
        INCOME_CLASSIFICATION_COLUMN: (
            "income_classification"
        ),
        POPULATION_COLUMN: "population_2024",
    }
)

clean_pdf = clean_pdf[
    [
        "psgc_code",
        "correspondence_code",
        "lgu_name",
        "province_code",
        "province_name",
        "geographic_level",
        "old_name",
        "city_class",
        "income_classification",
        "population_2024",
    ]
].copy()

text_columns = [
    "psgc_code",
    "correspondence_code",
    "lgu_name",
    "province_code",
    "province_name",
    "geographic_level",
    "old_name",
    "city_class",
    "income_classification",
]


# =====================================================
# CLEAN SOURCE VALUES
# =====================================================

for column in text_columns:
    clean_pdf[column] = (
        clean_pdf[column]
        .astype("string")
        .str.strip()
    )

clean_pdf["population_2024"] = pd.to_numeric(
    clean_pdf["population_2024"],
    errors="coerce",
)


# =====================================================
# CREATE PYTHON RECORDS
# =====================================================

source_records = []

for record in clean_pdf.to_dict(
    orient="records"
):
    cleaned_record = {}

    for column in text_columns:
        value = record[column]

        if pd.isna(value):
            cleaned_record[column] = None
        else:
            cleaned_record[column] = str(
                value
            ).strip()

    population_value = record[
        "population_2024"
    ]

    if pd.isna(population_value):
        cleaned_record["population_2024"] = None
    else:
        cleaned_record["population_2024"] = int(
            population_value
        )

    source_records.append(
        cleaned_record
    )


# =====================================================
# CREATE SPARK DATAFRAME
# =====================================================

source_df = spark.createDataFrame(
    source_records,
    schema=LGU_SOURCE_SCHEMA,
)


# =====================================================
# VALIDATE REQUIRED VALUES
# =====================================================

invalid_required_count = (
    source_df
    .where(
        F.col("psgc_code").isNull()
        | (F.col("psgc_code") == "")
        | F.col("lgu_name").isNull()
        | (F.col("lgu_name") == "")
        | F.col("geographic_level").isNull()
        | (F.col("geographic_level") == "")
    )
    .count()
)

if invalid_required_count > 0:
    raise RuntimeError(
        "Found "
        + str(invalid_required_count)
        + " rows with missing required values"
    )


# =====================================================
# VALIDATE GEOGRAPHIC LEVELS
# =====================================================

invalid_level_count = (
    source_df
    .where(
        ~F.col("geographic_level").isin(
            "City",
            "Mun",
        )
    )
    .count()
)

if invalid_level_count > 0:
    raise RuntimeError(
        "Found "
        + str(invalid_level_count)
        + " invalid geographic levels"
    )


# =====================================================
# VALIDATE PSGC CODES
# =====================================================

invalid_code_count = (
    source_df
    .where(
        ~F.col("psgc_code").rlike(
            "^[0-9]{10}$"
        )
    )
    .count()
)

if invalid_code_count > 0:
    raise RuntimeError(
        "Found "
        + str(invalid_code_count)
        + " invalid PSGC codes"
    )

duplicate_code_count = (
    source_df
    .groupBy("psgc_code")
    .count()
    .where(
        F.col("count") > 1
    )
    .count()
)

if duplicate_code_count > 0:
    raise RuntimeError(
        "Found "
        + str(duplicate_code_count)
        + " duplicate PSGC codes"
    )


# =====================================================
# VALIDATE CLEANED COUNT
# =====================================================

validated_total_count = source_df.count()

if validated_total_count != EXPECTED_TOTAL_COUNT:
    raise RuntimeError(
        "Expected "
        + str(EXPECTED_TOTAL_COUNT)
        + " validated LGUs, found "
        + str(validated_total_count)
    )

print("Source data validation passed")

print(
    "Validated LGUs: "
    + str(validated_total_count)
)


# =====================================================
# ADD REFERENCE METADATA
# =====================================================

final_df = (
    source_df
    .withColumn(
        "source_file",
        F.lit(SOURCE_FILE),
    )
    .withColumn(
        "source_publication_date",
        F.to_date(
            F.lit(SOURCE_PUBLICATION_DATE)
        ),
    )
    .withColumn(
        "is_active",
        F.lit(True).cast(BooleanType()),
    )
    .withColumn(
        "created_timestamp",
        F.current_timestamp(),
    )
    .withColumn(
        "updated_timestamp",
        F.current_timestamp(),
    )
)


# =====================================================
# MATCH TARGET COLUMN ORDER
# =====================================================

final_df = final_df.select(
    *target_df.columns
)

# =====================================================
# FINAL PRE-WRITE VALIDATION
# =====================================================

final_counts = {
    row["geographic_level"]: row["count"]
    for row in (
        final_df
        .groupBy("geographic_level")
        .count()
        .collect()
    )
}

final_city_count = final_counts.get(
    "City",
    0,
)

final_municipality_count = final_counts.get(
    "Mun",
    0,
)

final_source_count = final_df.count()

if final_city_count != EXPECTED_CITY_COUNT:
    raise RuntimeError(
        "Expected "
        + str(EXPECTED_CITY_COUNT)
        + " cities, found "
        + str(final_city_count)
    )

if (
    final_municipality_count
    != EXPECTED_MUNICIPALITY_COUNT
):
    raise RuntimeError(
        "Expected "
        + str(EXPECTED_MUNICIPALITY_COUNT)
        + " municipalities, found "
        + str(final_municipality_count)
    )

if final_source_count != EXPECTED_TOTAL_COUNT:
    raise RuntimeError(
        "Expected "
        + str(EXPECTED_TOTAL_COUNT)
        + " total LGUs, found "
        + str(final_source_count)
    )

print("Final pre-write validation passed")


# =====================================================
# REPLACE CURRENT REFERENCE SNAPSHOT
# =====================================================

(
    final_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "false",
    )
    .saveAsTable(TARGET_TABLE)
)


# =====================================================
# VERIFY SAVED TABLE
# =====================================================

saved_df = spark.table(
    TARGET_TABLE
)

saved_counts = {
    row["geographic_level"]: row["count"]
    for row in (
        saved_df
        .groupBy("geographic_level")
        .count()
        .collect()
    )
}

saved_city_count = saved_counts.get(
    "City",
    0,
)

saved_municipality_count = saved_counts.get(
    "Mun",
    0,
)

saved_total_count = saved_df.count()

saved_province_linked_count = (
    saved_df
    .where(
        F.col("province_code").isNotNull()
        & F.col("province_name").isNotNull()
    )
    .count()
)

saved_no_province_count = (
    saved_df
    .where(
        F.col("province_code").isNull()
        & F.col("province_name").isNull()
    )
    .count()
)

saved_incomplete_province_count = (
    saved_df
    .where(
        F.col("province_code").isNull()
        != F.col("province_name").isNull()
    )
    .count()
)

if (
    saved_province_linked_count
    != EXPECTED_PROVINCE_LINKED_COUNT
):
    raise RuntimeError(
        "Expected "
        + str(EXPECTED_PROVINCE_LINKED_COUNT)
        + " saved province-linked LGUs, found "
        + str(saved_province_linked_count)
    )

if (
    saved_no_province_count
    != EXPECTED_NO_PROVINCE_COUNT
):
    raise RuntimeError(
        "Expected "
        + str(EXPECTED_NO_PROVINCE_COUNT)
        + " saved LGUs without province, found "
        + str(saved_no_province_count)
    )

if saved_incomplete_province_count > 0:
    raise RuntimeError(
        "Post-write validation found "
        + str(saved_incomplete_province_count)
        + " incomplete province pairs"
    )

if saved_city_count != EXPECTED_CITY_COUNT:
    raise RuntimeError(
        "Expected "
        + str(EXPECTED_CITY_COUNT)
        + " saved cities, found "
        + str(saved_city_count)
    )

if (
    saved_municipality_count
    != EXPECTED_MUNICIPALITY_COUNT
):
    raise RuntimeError(
        "Expected "
        + str(EXPECTED_MUNICIPALITY_COUNT)
        + " saved municipalities, found "
        + str(saved_municipality_count)
    )

if saved_total_count != EXPECTED_TOTAL_COUNT:
    raise RuntimeError(
        "Expected "
        + str(EXPECTED_TOTAL_COUNT)
        + " saved LGUs, found "
        + str(saved_total_count)
    )


# =====================================================
# VERIFY SAVED UNIQUENESS
# =====================================================

saved_duplicate_count = (
    saved_df
    .groupBy("psgc_code")
    .count()
    .where(
        F.col("count") > 1
    )
    .count()
)

if saved_duplicate_count > 0:
    raise RuntimeError(
        "Post-write validation found duplicate PSGC codes"
    )


# =====================================================
# VERIFY SAVED METADATA
# =====================================================

invalid_metadata_count = (
    saved_df
    .where(
        F.col("source_file").isNull()
        | (F.col("source_file") == "")
        | F.col(
            "source_publication_date"
        ).isNull()
        | F.col("is_active").isNull()
        | F.col(
            "created_timestamp"
        ).isNull()
        | F.col(
            "updated_timestamp"
        ).isNull()
    )
    .count()
)

if invalid_metadata_count > 0:
    raise RuntimeError(
        "Post-write validation found "
        + str(invalid_metadata_count)
        + " records with missing metadata"
    )


# =====================================================
# LOAD SUMMARY
# =====================================================

print()
print("LGU MASTER LOAD COMPLETE")

print(
    "Cities saved: "
    + str(saved_city_count)
)

print(
    "Municipalities saved: "
    + str(saved_municipality_count)
)

print(
    "Total LGUs saved: "
    + str(saved_total_count)
)

print(
    "Province-linked LGUs: "
    + str(saved_province_linked_count)
)

print(
    "LGUs without province assignment: "
    + str(saved_no_province_count)
)

print(
    "Duplicate PSGC codes: "
    + str(saved_duplicate_count)
)

print(
    "Source file: "
    + SOURCE_FILE
)

print(
    "Source publication date: "
    + SOURCE_PUBLICATION_DATE
)
