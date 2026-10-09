import os

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------

PORTAL_URL = os.environ["AHON_CMCI_PORTAL_URL"]
PROCESS_URL = os.environ["AHON_CMCI_PROCESS_URL"]
CMCI_YEARS = tuple(
    year.strip() for year in os.environ["AHON_CMCI_YEARS"].split(",") if year.strip()
)
if (
    not CMCI_YEARS
    or len(CMCI_YEARS) != len(set(CMCI_YEARS))
    or any(not year.isdigit() for year in CMCI_YEARS)
):
    raise ValueError("AHON_CMCI_YEARS must contain unique numeric reporting years")

REQUEST_TIMEOUT_SECONDS = 60

# ------------------------------------------------------------------
# SOURCE PROVENANCE
# ------------------------------------------------------------------

SOURCE_NAME = "cmci_indicator_batch_html"

# PROCESS_URL currently contains no query parameters, keys, or tokens.
# This reference can safely be stored in Bronze provenance metadata.
SOURCE_REF = PROCESS_URL

# ------------------------------------------------------------------
# APPROVED INDICATORS BY PILLAR
# ------------------------------------------------------------------

INDICATORS_BY_PILLAR = {
    "Economic Dynamism": {
        "Local Economy Size": "les",
        "Local Economy Growth": "leg",
        "Active Establishments in the Locality": "sle",
        "Employment Generation": "job",
    },
    "Government Efficiency": {
        "Compliance to National Directives": "cnd",
        "Presence of Investment Promotion Unit": "pipu",
        "Compliance to ARTA Citizens Charter": "bre",
        "Capacity to Generate Local Resource": "rat",
        "Capacity of Health Services": "chs",
        "Capacity of School Services": "css",
        "Recognition of Performance": "lgua",
        "Getting Business Permits": "bpls",
        "Peace and Order": "pao",
        "Social Protection": "sp",
    },
    "Infrastructure": {
        "Road Network": "road",
        "Distance to Ports": "dtp",
        "Availability of Basic Utilities": "abu",
        "Transportation Vehicles": "trans",
        "Education": "edu",
        "Health": "hea",
        "LGU Investment": "inv",
        "Accommodation Capacity": "acc",
        "Information Technology Capacity": "ict",
        "Financial Technology Capacity": "atm",
    },
    "Resiliency": {
        "Land Use Plan": "lup",
        "Disaster Risk Reduction Plan": "drrp",
        "Annual Disaster Drill": "add",
        "Early Warning System": "ews",
        "Budget for DRRMP": "drrmp",
        "Local Risk Assessments": "lra",
        "Emergency Infrastructure": "ei",
        "Utilities": "util",
        "Employed Population": "ep",
        "Sanitary System": "ss",
    },
    "Innovation": {
        "Internet Capability": "intc",
    },
}

# ------------------------------------------------------------------
# FLATTEN INDICATOR CONFIG
# ------------------------------------------------------------------

APPROVED_INDICATORS = {}
INDICATOR_PILLAR_LOOKUP = {}

for pillar_name, pillar_indicators in (
    INDICATORS_BY_PILLAR.items()
):
    for indicator_label, indicator_code in (
        pillar_indicators.items()
    ):
        APPROVED_INDICATORS[
            indicator_label
        ] = indicator_code

        INDICATOR_PILLAR_LOOKUP[
            indicator_label
        ] = pillar_name

EXPECTED_INDICATOR_COUNT = len(
    APPROVED_INDICATORS
)

REQUESTED_INDICATOR_CODES = list(
    APPROVED_INDICATORS.values()
)

EXPECTED_INDICATOR_LABELS = set(
    APPROVED_INDICATORS.keys()
)

# ------------------------------------------------------------------
# VALIDATE INDICATOR CONFIG
# ------------------------------------------------------------------

if EXPECTED_INDICATOR_COUNT != 35:
    raise RuntimeError(
        "Expected 35 approved indicators, found "
        + str(EXPECTED_INDICATOR_COUNT)
    )

if (
    len(set(REQUESTED_INDICATOR_CODES))
    != EXPECTED_INDICATOR_COUNT
):
    raise RuntimeError(
        "Approved indicator codes contain duplicates"
    )

if (
    len(EXPECTED_INDICATOR_LABELS)
    != EXPECTED_INDICATOR_COUNT
):
    raise RuntimeError(
        "Approved indicator labels contain duplicates"
    )

for indicator_label, pillar_name in (
    INDICATOR_PILLAR_LOOKUP.items()
):
    if indicator_label not in APPROVED_INDICATORS:
        raise RuntimeError(
            "Indicator pillar lookup contains an "
            + "unapproved indicator: "
            + indicator_label
        )

    if pillar_name not in INDICATORS_BY_PILLAR:
        raise RuntimeError(
            "Indicator pillar lookup contains an "
            + "unknown pillar: "
            + pillar_name
        )

# ------------------------------------------------------------------
# CREATE HTTP SESSION
# ------------------------------------------------------------------

def create_http_session():
    """
    Create a reusable HTTP session with controlled retries.
    """

    retry_strategy = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=2,
        status_forcelist=(
            429,
            500,
            502,
            503,
            504,
        ),
        allowed_methods=(
            "GET",
            "POST",
        ),
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy
    )

    session = requests.Session()

    session.mount(
        "https://",
        adapter,
    )

    session.mount(
        "http://",
        adapter,
    )

    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/131.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,"
                "application/xhtml+xml,"
                "application/xml;q=0.9,"
                "*/*;q=0.8"
            ),
            "Accept-Language": (
                "en-US,en;q=0.9"
            ),
            "Referer": PORTAL_URL,
        }
    )

    return session
