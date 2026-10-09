import os
import re
import unicodedata

from bs4 import BeautifulSoup
from cmci_common import (
    PORTAL_URL,
    REQUEST_TIMEOUT_SECONDS,
    create_http_session,
)
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.getOrCreate()

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------

CATALOG = os.environ.get("AHON_CATALOG", "ahon")

LGU_MASTER_TABLE = (
    f"{CATALOG}.reference.lgu_master"
)

CMCI_MAP_TABLE = (
    f"{CATALOG}.reference.cmci_lgu_map"
)


EXPECTED_PROVINCE_SUFFIX_COUNT


EXPECTED_PROVINCE_SUFFIX_COUNT = 347
EXPECTED_SUFFIX_EXCEPTION_COUNT = 4
EXPECTED_MANUAL_ALIAS_COUNT = 20
EXPECTED_UPDATE_COUNT = 371

EXPECTED_INITIAL_MATCHED_COUNT = 1263
EXPECTED_INITIAL_UNMATCHED_COUNT = 379

EXPECTED_FINAL_MATCHED_COUNT = 1634
EXPECTED_FINAL_UNMATCHED_COUNT = 8

# ------------------------------------------------------------------
# Reviewed CMCI qualifier-to-province codebook
# ------------------------------------------------------------------

CMCI_SUFFIX_PROVINCE_LOOKUP = {
    "AA": "Abra",
    "AE": "Antique",
    "AK": "Aklan",
    "AN": "Agusan del Norte",
    "AO": "Apayao",
    "AS": "Agusan del Sur",
    "AU": "Aurora",
    "AY": "Albay",
    "BA": "Basilan",
    "BAS": "Basilan",
    "BK": "Bukidnon",
    "BL": "Bohol",
    "BN": "Bataan",
    "BS": "Batangas",
    "BU": "Bulacan",
    "CE": "Cavite",
    "CG": "Cagayan",
    "CM": "Camiguin",
    "CN": "Camarines Norte",
    "CS": "Camarines Sur",
    "CT": "Catanduanes",
    "CU": "Cebu",
    "CV": "Davao de Oro",
    "CZ": "Capiz",
    "DDO": "Davao de Oro",
    "DI": "Dinagat Islands",
    "DN": "Davao del Norte",
    "DO": "Davao Oriental",
    "ES": "Eastern Samar",
    "GS": "Guimaras",
    "IA": "Isabela",
    "IMELDA": "Romblon",
    "IN": "Ilocos Norte",
    "IO": "Iloilo",
    "IS": "Ilocos Sur",
    "KA": "Kalinga",
    "LA": "Laguna",
    "LE": "Leyte",
    "LN": "Lanao del Norte",
    "LS": "Lanao del Sur",
    "LU": "La Union",
    "MA": "Maguindanao del Norte",
    "MC": "Misamis Occidental",
    "ME": "Marinduque",
    "MM": "Metro Manila",
    "MO": "Misamis Oriental",
    "MP": "Mountain Province",
    "MS": "Masbate",
    "NC": "Cotabato",
    "NE": "Nueva Ecija",
    "NO": "Negros Occidental",
    "NR": "Negros Oriental",
    "NS": "Northern Samar",
    "NV": "Nueva Vizcaya",
    "OM": "Occidental Mindoro",
    "OR": "Oriental Mindoro",
    "PA": "Pampanga",
    "PN": "Palawan",
    "PS": "Pangasinan",
    "QN": "Quezon",
    "RL": "Rizal",
    "RN": "Romblon",
    "SC": "South Cotabato",
    "SK": "Sultan Kudarat",
    "SL": "Southern Leyte",
    "SN": "Surigao del Norte",
    "SO": "Sorsogon",
    "SR": "Siquijor",
    "SS": "Surigao del Sur",
    "SU": "Sulu",
    "TC": "Tarlac",
    "WS": "Samar",
    "ZA": "Zambales",
    "ZN": "Zamboanga del Norte",
    "ZR": "Zamboanga del Sur",
    "ZS": "Zamboanga Sibugay",
}

CMCI_CONFLICTING_SUFFIXES = {
    "DS": [
        "Davao del Sur",
        "Davao Occidental",
    ],
}

CMCI_CONFLICTING_SUFFIX_EXCEPTIONS = {
    (
        "hagonoy",
        "davao del sur",
    ): "Hagonoy (DS)",
    (
        "magsaysay",
        "davao del sur",
    ): "Magsaysay (DS)",
    (
        "santa cruz",
        "davao del sur",
    ): "Santa Cruz (DS)",
    (
        "santa maria",
        "davao occidental",
    ): "Santa Maria (DS)",
}

CMCI_REVIEWED_MANUAL_ALIASES = {
    "0301405000": "Bulakan",
    "0504116000": "Pio V Corpuz",
    "0701235000": "President Garcia",
    "0702209000": "Bantayan Island",
    "0907211000": "Roxas (ZN)",
    "0907214000": "Sergio Osmena",
    "0907226000": "Bacungan Leon B. Postigo",
    "0908312000": "R.T. Lim",
    "0990101000": "Isabela (BAS)",
    "1004217000": "Don Victoriano Chiongbian",
    "1102317000": "Igacos",
    "1102324000": "San Isidro (DN)",
    "1204711000": "Pigcawayan",
    "1206512000": "Senator Ninoy Aquino",
    "1381300000": "Quezon (MM)",
    "1381400000": "San Juan (MM)",
    "1705323000": "Rizal (PN)",
    "1804508000": "E. B. Magalona",
    "1830200000": "Bacolod (NO)",
    "1908704000": "Datu Blah Sinsuat",
}

MAPPING_UPDATE_SCHEMA = StructType(
    [
        StructField(
            "psgc_code",
            StringType(),
            nullable=False,
        ),
        StructField(
            "cmci_name",
            StringType(),
            nullable=False,
        ),
        StructField(
            "match_method",
            StringType(),
            nullable=False,
        ),
    ]
)

# ------------------------------------------------------------------
# Candidate name normalization
# ------------------------------------------------------------------

ABBREVIATION_PATTERNS = [
    (
        r"\bsta\.?\b",
        "santa",
    ),
    (
        r"\bsto\.?\b",
        "santo",
    ),
    (
        r"\bst\.?\b",
        "saint",
    ),
    (
        r"\bgen\.?\b",
        "general",
    ),
    (
        r"\bgov\.?\b",
        "governor",
    ),
    (
        r"\bpres\.?\b",
        "president",
    ),
    (
        r"\bmt\.?\b",
        "mount",
    ),
]


def normalize_candidate_name(
    name,
):
    """
    Normalize an LGU name for controlled mapping comparison.
    """

    if name is None:
        return None

    normalized_name = unicodedata.normalize(
        "NFKD",
        str(name),
    )

    normalized_name = "".join(
        character
        for character in normalized_name
        if not unicodedata.combining(
            character
        )
    )

    normalized_name = (
        normalized_name
        .lower()
        .strip()
    )

    normalized_name = re.sub(
        r"^municipality of\s+",
        "",
        normalized_name,
    )

    normalized_name = re.sub(
        r"^city of\s+",
        "",
        normalized_name,
    )

    normalized_name = re.sub(
        r"\s+city$",
        "",
        normalized_name,
    )

    for pattern, replacement in (
        ABBREVIATION_PATTERNS
    ):
        normalized_name = re.sub(
            pattern,
            replacement,
            normalized_name,
        )

    normalized_name = re.sub(
        r"[^a-z0-9]+",
        " ",
        normalized_name,
    )

    normalized_name = re.sub(
        r"\s+",
        " ",
        normalized_name,
    ).strip()

    return normalized_name


def split_cmci_name_suffix(
    cmci_name,
):
    """
    Separate the CMCI base locality name and trailing qualifier.
    """

    if cmci_name is None:
        return None, None

    cleaned_name = str(
        cmci_name
    ).strip()

    suffix_match = re.search(
        r"\s*\(([^()]+)\)\s*$",
        cleaned_name,
    )

    if suffix_match is None:
        return cleaned_name, None

    cmci_suffix = suffix_match.group(
        1
    ).strip().upper()

    cmci_base_name = cleaned_name[
        :suffix_match.start()
    ].strip()

    return cmci_base_name, cmci_suffix

# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------

def main():
    spark.conf.set(
        "spark.sql.session.timeZone",
        "UTC",
    )

    print("CMCI mapping update validation started")
    print("Target: " + CMCI_MAP_TABLE)


    # ------------------------------------------------------------------
    # Validate required tables
    # ------------------------------------------------------------------

    required_tables = [
        LGU_MASTER_TABLE,
        CMCI_MAP_TABLE,
    ]

    for table_name in required_tables:
        if not spark.catalog.tableExists(
            table_name
        ):
            raise RuntimeError(
                "Required table does not exist: "
                + table_name
            )

    print("Required tables found")


    # ------------------------------------------------------------------
    # Read current reference tables
    # ------------------------------------------------------------------

    active_lgu_df = (
        spark.table(
            LGU_MASTER_TABLE
        )
        .where(
            F.col("is_active") == True
        )
    )

    mapping_df = spark.table(
        CMCI_MAP_TABLE
    )

    mapping_total_count = mapping_df.count()

    if mapping_total_count != 1642:
        raise RuntimeError(
            "Expected 1642 mapping rows, found "
            + str(mapping_total_count)
        )

    current_matched_count = (
        mapping_df
        .where(
            F.col("match_status") == "MATCHED"
        )
        .where(
            F.col("reviewed") == True
        )
        .where(
            F.col("is_active") == True
        )
        .count()
    )

    current_unmatched_count = (
        mapping_df
        .where(
            F.col("match_status") == "UNMATCHED"
        )
        .count()
    )

    print(
        "Current approved mappings: "
        + str(current_matched_count)
    )

    print(
        "Current unmatched mappings: "
        + str(current_unmatched_count)
    )

    # ------------------------------------------------------------------
    # VALIDATE MAPPING MIGRATION STATE
    # ------------------------------------------------------------------

    if (
        current_matched_count == EXPECTED_FINAL_MATCHED_COUNT
        and current_unmatched_count
        == EXPECTED_FINAL_UNMATCHED_COUNT
    ):
        print()
        print("CMCI mapping update is already complete.")

        print(
            "Matched mappings: "
            + str(current_matched_count)
        )

        print(
            "Unmatched mappings: "
            + str(current_unmatched_count)
        )

        print("No Delta MERGE was executed.")

        return

    if (
        current_matched_count
        != EXPECTED_INITIAL_MATCHED_COUNT
        or current_unmatched_count
        != EXPECTED_INITIAL_UNMATCHED_COUNT
    ):
        raise RuntimeError(
            "CMCI mapping table is in an unexpected state. "
            + "Expected "
            + str(EXPECTED_INITIAL_MATCHED_COUNT)
            + " matched and "
            + str(EXPECTED_INITIAL_UNMATCHED_COUNT)
            + " unmatched before migration, or "
            + str(EXPECTED_FINAL_MATCHED_COUNT)
            + " matched and "
            + str(EXPECTED_FINAL_UNMATCHED_COUNT)
            + " unmatched after migration. Found "
            + str(current_matched_count)
            + " matched and "
            + str(current_unmatched_count)
            + " unmatched."
        )

    # ------------------------------------------------------------------
    # Read live CMCI locality names
    # ------------------------------------------------------------------

    session = create_http_session()

    try:
        portal_response = session.get(
            PORTAL_URL,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

        portal_response.raise_for_status()

        portal_html = portal_response.text.strip()

    finally:
        session.close()

    if not portal_html:
        raise RuntimeError(
            "CMCI portal returned an empty response"
        )

    portal_soup = BeautifulSoup(
        portal_html,
        "html.parser",
    )

    lgu_select = portal_soup.find(
        "select",
        attrs={
            "id": "lgu",
        },
    )

    if lgu_select is None:
        raise RuntimeError(
            "CMCI LGU selector was not found"
        )

    cmci_names = []

    for option in lgu_select.find_all(
        "option"
    ):
        option_value = option.get(
            "value"
        )

        if option_value is None:
            continue

        option_value = option_value.strip()

        if option_value:
            cmci_names.append(
                option_value
            )

    cmci_names = list(
        dict.fromkeys(
            cmci_names
        )
    )

    cmci_name_set = set(
        cmci_names
    )

    if len(cmci_names) != 1634:
        raise RuntimeError(
            "Expected 1634 live CMCI locality names, found "
            + str(len(cmci_names))
        )

    print(
        "Live CMCI locality names: "
        + str(len(cmci_names))
    )


    # ------------------------------------------------------------------
    # Prepare currently approved and unused CMCI names
    # ------------------------------------------------------------------

    approved_mapping_df = (
        mapping_df
        .where(
            F.col("match_status") == "MATCHED"
        )
        .where(
            F.col("reviewed") == True
        )
        .where(
            F.col("is_active") == True
        )
    )

    approved_cmci_names = {
        row["cmci_name"]
        for row in (
            approved_mapping_df
            .select(
                "cmci_name"
            )
            .where(
                F.col("cmci_name").isNotNull()
            )
            .collect()
        )
    }

    unused_cmci_names = sorted(
        cmci_name_set
        - approved_cmci_names
    )

    if len(unused_cmci_names) != 371:
        raise RuntimeError(
            "Expected 371 unused CMCI names, found "
            + str(len(unused_cmci_names))
        )

    print(
        "Unused CMCI locality names: "
        + str(len(unused_cmci_names))
    )


    # ------------------------------------------------------------------
    # Prepare currently unmatched PSGC rows
    # ------------------------------------------------------------------

    unmatched_rows = (
        mapping_df
        .where(
            F.col("match_status") == "UNMATCHED"
        )
        .alias(
            "mapping"
        )
        .join(
            active_lgu_df
            .select(
                "psgc_code",
                "province_code",
                "province_name",
            )
            .alias(
                "master"
            ),
            on="psgc_code",
            how="left",
        )
        .select(
            F.col("psgc_code"),
            F.col("mapping.psgc_name").alias(
                "psgc_name"
            ),
            F.col("master.province_code").alias(
                "province_code"
            ),
            F.col("master.province_name").alias(
                "province_name"
            ),
        )
        .orderBy(
            "psgc_code"
        )
        .collect()
    )

    if len(unmatched_rows) != 379:
        raise RuntimeError(
            "Expected 379 unmatched PSGC rows, found "
            + str(len(unmatched_rows))
        )

    print(
        "Unmatched PSGC rows: "
        + str(len(unmatched_rows))
    )


    # ------------------------------------------------------------------
    # Generate province-suffix updates
    # ------------------------------------------------------------------

    province_suffix_updates = []

    for cmci_name in unused_cmci_names:
        cmci_base_name, cmci_suffix = (
            split_cmci_name_suffix(
                cmci_name
            )
        )

        if cmci_suffix is None:
            continue

        if cmci_suffix in CMCI_CONFLICTING_SUFFIXES:
            continue

        suffix_province_name = (
            CMCI_SUFFIX_PROVINCE_LOOKUP.get(
                cmci_suffix
            )
        )

        if suffix_province_name is None:
            continue

        normalized_cmci_base_name = (
            normalize_candidate_name(
                cmci_base_name
            )
        )

        matching_psgc_rows = []

        for unmatched_row in unmatched_rows:
            if unmatched_row["province_name"] is None:
                continue

            normalized_psgc_name = (
                normalize_candidate_name(
                    unmatched_row["psgc_name"]
                )
            )

            normalized_psgc_province = (
                normalize_candidate_name(
                    unmatched_row["province_name"]
                )
            )

            normalized_cmci_province = (
                normalize_candidate_name(
                    suffix_province_name
                )
            )

            if (
                normalized_psgc_name
                == normalized_cmci_base_name
                and normalized_psgc_province
                == normalized_cmci_province
            ):
                matching_psgc_rows.append(
                    unmatched_row
                )

        if len(matching_psgc_rows) != 1:
            continue

        matched_row = matching_psgc_rows[0]

        province_suffix_updates.append(
            {
                "psgc_code": matched_row[
                    "psgc_code"
                ],
                "cmci_name": cmci_name,
                "match_method": (
                    "PROVINCE_SUFFIX"
                ),
            }
        )

    if (
        len(province_suffix_updates)
        != EXPECTED_PROVINCE_SUFFIX_COUNT
    ):
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_PROVINCE_SUFFIX_COUNT)
            + " province-suffix updates, found "
            + str(
                len(
                    province_suffix_updates
                )
            )
        )

    print(
        "Province-suffix updates: "
        + str(
            len(
                province_suffix_updates
            )
        )
    )


    # ------------------------------------------------------------------
    # Generate reviewed DS exception updates
    # ------------------------------------------------------------------

    suffix_exception_updates = []

    unused_cmci_name_set = set(
        unused_cmci_names
    )

    for unmatched_row in unmatched_rows:
        if unmatched_row["province_name"] is None:
            continue

        exception_key = (
            normalize_candidate_name(
                unmatched_row["psgc_name"]
            ),
            normalize_candidate_name(
                unmatched_row["province_name"]
            ),
        )

        cmci_name = (
            CMCI_CONFLICTING_SUFFIX_EXCEPTIONS.get(
                exception_key
            )
        )

        if cmci_name is None:
            continue

        if cmci_name not in unused_cmci_name_set:
            raise RuntimeError(
                "Reviewed suffix exception is not an "
                + "unused CMCI name: "
                + cmci_name
            )

        suffix_exception_updates.append(
            {
                "psgc_code": unmatched_row[
                    "psgc_code"
                ],
                "cmci_name": cmci_name,
                "match_method": (
                    "PROVINCE_SUFFIX_EXCEPTION"
                ),
            }
        )

    if (
        len(suffix_exception_updates)
        != EXPECTED_SUFFIX_EXCEPTION_COUNT
    ):
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_SUFFIX_EXCEPTION_COUNT)
            + " suffix-exception updates, found "
            + str(
                len(
                    suffix_exception_updates
                )
            )
        )

    print(
        "Suffix-exception updates: "
        + str(
            len(
                suffix_exception_updates
            )
        )
    )


    # ------------------------------------------------------------------
    # Generate reviewed manual-alias updates
    # ------------------------------------------------------------------

    manual_alias_updates = []

    unmatched_psgc_code_set = {
        row["psgc_code"]
        for row in unmatched_rows
    }

    for (
        psgc_code,
        cmci_name,
    ) in CMCI_REVIEWED_MANUAL_ALIASES.items():

        if psgc_code not in unmatched_psgc_code_set:
            raise RuntimeError(
                "Reviewed manual PSGC code is not "
                + "currently unmatched: "
                + psgc_code
            )

        if cmci_name not in cmci_name_set:
            raise RuntimeError(
                "Reviewed manual CMCI name is missing "
                + "from the live portal: "
                + cmci_name
            )

        if cmci_name not in unused_cmci_name_set:
            raise RuntimeError(
                "Reviewed manual CMCI name is already "
                + "assigned: "
                + cmci_name
            )

        manual_alias_updates.append(
            {
                "psgc_code": psgc_code,
                "cmci_name": cmci_name,
                "match_method": "MANUAL_ALIAS",
            }
        )

    if (
        len(manual_alias_updates)
        != EXPECTED_MANUAL_ALIAS_COUNT
    ):
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_MANUAL_ALIAS_COUNT)
            + " manual alias updates, found "
            + str(
                len(
                    manual_alias_updates
                )
            )
        )

    print(
        "Manual-alias updates: "
        + str(
            len(
                manual_alias_updates
            )
        )
    )


    # ------------------------------------------------------------------
    # Combine proposed updates
    # ------------------------------------------------------------------

    mapping_update_records = (
        province_suffix_updates
        + suffix_exception_updates
        + manual_alias_updates
    )

    if (
        len(mapping_update_records)
        != EXPECTED_UPDATE_COUNT
    ):
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_UPDATE_COUNT)
            + " total updates, found "
            + str(
                len(
                    mapping_update_records
                )
            )
        )


    # ------------------------------------------------------------------
    # Validate proposed update uniqueness
    # ------------------------------------------------------------------

    proposed_psgc_codes = [
        record["psgc_code"]
        for record in mapping_update_records
    ]

    proposed_cmci_names = [
        record["cmci_name"]
        for record in mapping_update_records
    ]

    duplicate_proposed_psgc_count = (
        len(proposed_psgc_codes)
        - len(set(proposed_psgc_codes))
    )

    duplicate_proposed_cmci_count = (
        len(proposed_cmci_names)
        - len(set(proposed_cmci_names))
    )

    if duplicate_proposed_psgc_count > 0:
        raise RuntimeError(
            "Proposed updates contain duplicate PSGC codes: "
            + str(duplicate_proposed_psgc_count)
        )

    if duplicate_proposed_cmci_count > 0:
        raise RuntimeError(
            "Proposed updates contain duplicate CMCI names: "
            + str(duplicate_proposed_cmci_count)
        )

    overlapping_approved_cmci_names = (
        set(proposed_cmci_names)
        & approved_cmci_names
    )

    if overlapping_approved_cmci_names:
        raise RuntimeError(
            "Proposed updates reuse approved CMCI names: "
            + str(
                sorted(
                    overlapping_approved_cmci_names
                )
            )
        )


    # ------------------------------------------------------------------
    # Create proposed update DataFrame
    # ------------------------------------------------------------------

    mapping_update_df = spark.createDataFrame(
        mapping_update_records,
        schema=MAPPING_UPDATE_SCHEMA,
    )


    # ------------------------------------------------------------------
    # Validate proposed rows against target
    # ------------------------------------------------------------------

    non_unmatched_target_count = (
        mapping_update_df
        .alias(
            "source"
        )
        .join(
            mapping_df.alias(
                "target"
            ),
            on="psgc_code",
            how="inner",
        )
        .where(
            F.col("target.match_status")
            != "UNMATCHED"
        )
        .count()
    )

    if non_unmatched_target_count > 0:
        raise RuntimeError(
            "Proposed updates include "
            + str(non_unmatched_target_count)
            + " target rows that are not UNMATCHED"
        )

    missing_target_count = (
        mapping_update_df
        .select(
            "psgc_code"
        )
        .join(
            mapping_df.select(
                "psgc_code"
            ),
            on="psgc_code",
            how="left_anti",
        )
        .count()
    )

    if missing_target_count > 0:
        raise RuntimeError(
            "Proposed updates include "
            + str(missing_target_count)
            + " PSGC codes missing from the target table"
        )


    # ------------------------------------------------------------------
    # Preview dry-run summary
    # ------------------------------------------------------------------

    update_method_rows = (
        mapping_update_df
        .groupBy(
            "match_method"
        )
        .count()
        .orderBy(
            "match_method"
        )
        .collect()
    )

    print()
    print("CMCI MAPPING UPDATE DRY RUN PASSED")

    print(
        "Proposed update count: "
        + str(
            mapping_update_df.count()
        )
    )

    for row in update_method_rows:
        print(
            str(row["match_method"])
            + ": "
            + str(row["count"])
        )

    print(
        "Expected final matched count: "
        + str(
            current_matched_count
            + EXPECTED_UPDATE_COUNT
        )
    )

    print(
        "Expected final unmatched count: "
        + str(
            current_unmatched_count
            - EXPECTED_UPDATE_COUNT
        )
    )

    print()
    print(
        "No mapping records were changed. "
        + "Delta MERGE has not been executed."
    )

    # ------------------------------------------------------------------
    # Capture target version before update
    # ------------------------------------------------------------------

    target_history_before = (
        spark.sql(
            "DESCRIBE HISTORY "
            + CMCI_MAP_TABLE
        )
        .select(
            "version"
        )
        .orderBy(
            F.col("version").desc()
        )
        .first()
    )

    target_version_before = int(
        target_history_before["version"]
    )

    print()
    print(
        "Target Delta version before update: "
        + str(target_version_before)
    )


    # ------------------------------------------------------------------
    # Final immediate pre-write validation
    # ------------------------------------------------------------------

    pre_write_unmatched_count = (
        mapping_update_df
        .alias(
            "source"
        )
        .join(
            spark.table(
                CMCI_MAP_TABLE
            ).alias(
                "target"
            ),
            on="psgc_code",
            how="inner",
        )
        .where(
            F.col("target.match_status")
            == "UNMATCHED"
        )
        .where(
            F.col("target.cmci_name").isNull()
        )
        .where(
            F.col("target.reviewed") == False
        )
        .where(
            F.col("target.is_active") == False
        )
        .count()
    )

    if pre_write_unmatched_count != EXPECTED_UPDATE_COUNT:
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_UPDATE_COUNT)
            + " immediately eligible target rows, found "
            + str(pre_write_unmatched_count)
        )

    print(
        "Final pre-write validation passed: "
        + str(pre_write_unmatched_count)
        + " rows eligible"
    )


    # ------------------------------------------------------------------
    # Execute safeguarded Delta MERGE
    # ------------------------------------------------------------------

    target_delta_table = DeltaTable.forName(
        spark,
        CMCI_MAP_TABLE,
    )

    (
        target_delta_table
        .alias(
            "target"
        )
        .merge(
            mapping_update_df.alias(
                "source"
            ),
            (
                "target.psgc_code = source.psgc_code "
                "AND target.match_status = 'UNMATCHED'"
            ),
        )
        .whenMatchedUpdate(
            set={
                "cmci_name": (
                    "source.cmci_name"
                ),
                "match_status": (
                    "'MATCHED'"
                ),
                "match_method": (
                    "source.match_method"
                ),
                "reviewed": (
                    "true"
                ),
                "is_active": (
                    "true"
                ),
                "updated_timestamp": (
                    "current_timestamp()"
                ),
            }
        )
        .execute()
    )

    print()
    print("Delta MERGE executed")


    # ------------------------------------------------------------------
    # Read updated mapping table
    # ------------------------------------------------------------------

    updated_mapping_df = spark.table(
        CMCI_MAP_TABLE
    )

    updated_total_count = (
        updated_mapping_df.count()
    )

    updated_matched_df = (
        updated_mapping_df
        .where(
            F.col("match_status") == "MATCHED"
        )
        .where(
            F.col("reviewed") == True
        )
        .where(
            F.col("is_active") == True
        )
    )

    updated_matched_count = (
        updated_matched_df.count()
    )

    updated_unmatched_df = (
        updated_mapping_df
        .where(
            F.col("match_status") == "UNMATCHED"
        )
    )

    updated_unmatched_count = (
        updated_unmatched_df.count()
    )


    # ------------------------------------------------------------------
    # Validate final counts
    # ------------------------------------------------------------------

    if updated_total_count != 1642:
        raise RuntimeError(
            "Expected 1642 total mapping rows after update, found "
            + str(updated_total_count)
        )

    if (
        updated_matched_count
        != EXPECTED_FINAL_MATCHED_COUNT
    ):
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_FINAL_MATCHED_COUNT)
            + " final matched mappings, found "
            + str(updated_matched_count)
        )

    if (
        updated_unmatched_count
        != EXPECTED_FINAL_UNMATCHED_COUNT
    ):
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_FINAL_UNMATCHED_COUNT)
            + " final unmatched mappings, found "
            + str(updated_unmatched_count)
        )


    # ------------------------------------------------------------------
    # Verify all proposed rows were updated
    # ------------------------------------------------------------------

    updated_proposed_count = (
        mapping_update_df
        .alias(
            "source"
        )
        .join(
            updated_mapping_df.alias(
                "target"
            ),
            on="psgc_code",
            how="inner",
        )
        .where(
            F.col("target.cmci_name")
            == F.col("source.cmci_name")
        )
        .where(
            F.col("target.match_method")
            == F.col("source.match_method")
        )
        .where(
            F.col("target.match_status")
            == "MATCHED"
        )
        .where(
            F.col("target.reviewed") == True
        )
        .where(
            F.col("target.is_active") == True
        )
        .count()
    )

    if updated_proposed_count != EXPECTED_UPDATE_COUNT:
        raise RuntimeError(
            "Expected "
            + str(EXPECTED_UPDATE_COUNT)
            + " proposed rows to be updated, verified "
            + str(updated_proposed_count)
        )


    # ------------------------------------------------------------------
    # Verify CMCI-name uniqueness
    # ------------------------------------------------------------------

    duplicate_active_cmci_count = (
        updated_matched_df
        .groupBy(
            "cmci_name"
        )
        .count()
        .where(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_active_cmci_count > 0:
        raise RuntimeError(
            "Post-update validation found "
            + str(duplicate_active_cmci_count)
            + " duplicate active CMCI names"
        )


    # ------------------------------------------------------------------
    # Verify PSGC-code uniqueness
    # ------------------------------------------------------------------

    duplicate_psgc_count = (
        updated_mapping_df
        .groupBy(
            "psgc_code"
        )
        .count()
        .where(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_psgc_count > 0:
        raise RuntimeError(
            "Post-update validation found "
            + str(duplicate_psgc_count)
            + " duplicate PSGC codes"
        )


    # ------------------------------------------------------------------
    # Verify matched-row completeness
    # ------------------------------------------------------------------

    invalid_matched_count = (
        updated_mapping_df
        .where(
            F.col("match_status") == "MATCHED"
        )
        .where(
            F.col("cmci_name").isNull()
            | (F.col("cmci_name") == "")
            | F.col("match_method").isNull()
            | (F.col("match_method") == "")
            | (F.col("reviewed") != True)
            | (F.col("is_active") != True)
            | F.col("updated_timestamp").isNull()
        )
        .count()
    )

    if invalid_matched_count > 0:
        raise RuntimeError(
            "Post-update validation found "
            + str(invalid_matched_count)
            + " incomplete matched records"
        )


    # ------------------------------------------------------------------
    # Verify remaining unmatched rows
    # ------------------------------------------------------------------

    invalid_unmatched_count = (
        updated_unmatched_df
        .where(
            F.col("cmci_name").isNotNull()
            | F.col("match_method").isNotNull()
            | (F.col("reviewed") != False)
            | (F.col("is_active") != False)
        )
        .count()
    )

    if invalid_unmatched_count > 0:
        raise RuntimeError(
            "Post-update validation found "
            + str(invalid_unmatched_count)
            + " invalid unmatched records"
        )


    # ------------------------------------------------------------------
    # Capture target version after update
    # ------------------------------------------------------------------

    target_history_after = (
        spark.sql(
            "DESCRIBE HISTORY "
            + CMCI_MAP_TABLE
        )
        .select(
            "version",
            "operation",
        )
        .orderBy(
            F.col("version").desc()
        )
        .first()
    )

    target_version_after = int(
        target_history_after["version"]
    )

    target_operation = str(
        target_history_after["operation"]
    )

    if target_version_after <= target_version_before:
        raise RuntimeError(
            "Delta table version did not advance after MERGE"
        )


    # ------------------------------------------------------------------
    # Final method counts
    # ------------------------------------------------------------------

    final_method_rows = (
        updated_matched_df
        .groupBy(
            "match_method"
        )
        .count()
        .orderBy(
            "match_method"
        )
        .collect()
    )


    # ------------------------------------------------------------------
    # Update summary
    # ------------------------------------------------------------------

    print()
    print("CMCI MAPPING UPDATE COMPLETE")

    print(
        "Delta version before: "
        + str(target_version_before)
    )

    print(
        "Delta version after: "
        + str(target_version_after)
    )

    print(
        "Latest Delta operation: "
        + target_operation
    )

    print(
        "Rows updated and verified: "
        + str(updated_proposed_count)
    )

    print(
        "Final matched mappings: "
        + str(updated_matched_count)
    )

    print(
        "Final unmatched mappings: "
        + str(updated_unmatched_count)
    )

    print(
        "Duplicate PSGC codes: "
        + str(duplicate_psgc_count)
    )

    print(
        "Duplicate active CMCI names: "
        + str(duplicate_active_cmci_count)
    )

    print()
    print("FINAL MATCH METHOD COUNTS")

    for row in final_method_rows:
        print(
            str(row["match_method"])
            + ": "
            + str(row["count"])
        )

    print()
    print("REMAINING UNMATCHED PSGC LGUS")

    remaining_unmatched_rows = (
        updated_unmatched_df
        .select(
            "psgc_code",
            "psgc_name",
        )
        .orderBy(
            "psgc_code"
        )
        .collect()
    )

    for row in remaining_unmatched_rows:
        print(
            str(row["psgc_code"])
            + " | "
            + str(row["psgc_name"])
        )

if __name__ == "__main__":
    main()  