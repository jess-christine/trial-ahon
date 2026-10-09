# Extract LDRRMF files to the source volume
# Copies the BLGF LDRRMF Excel files (FY2018 to FY2024) from Hugging Face into the source volume.
# Suggested repo path: src/sql/datasets/blgf_ldrrmf_annual_lgu/extract/extract_blgf_ldrrmf_annual_lgu.py
# What it does:
#   1. Downloads data/ldrrmf/* from the Hugging Face dataset repo to temporary local disk.
#   2. Copies the files, unchanged, into the source volume, in one folder named after the dataset.
#   3. Lists what landed, so you can check it.
# It does not read or change the Excel contents. Reading them into the bronze table is the next step.
# Run it as a file (Databricks job or `databricks bundle run`), not as a notebook.
# Requires the `huggingface_hub` package on the cluster (add it as a library on the job or cluster).

import os
import shutil
import tempfile
from pathlib import Path

from huggingface_hub import snapshot_download

# Hugging Face dataset repo and the folder in it that holds the LDRRMF files.
HF_REPO_ID = "Jess-Christine/Project-AHON"
HF_FOLDER = "data/ldrrmf"

# Destination: the source volume, one folder per dataset, named after the dataset
# (naming standard). The volume name is a stand-in and may change.
CATALOG = os.environ.get("AHON_CATALOG", "ahon")
VOLUME_ROOT = os.environ.get("AHON_SOURCE_VOLUME", f"/Volumes/{CATALOG}/reference/source")
DATASET_NAME = "blgf_ldrrmf_annual_lgu"
TARGET_DIR = Path(VOLUME_ROOT) / DATASET_NAME

# Download to temporary local disk
# A temp folder keeps Hugging Face's hidden .cache folder and the extra
# data/ldrrmf nesting out of the volume.
tmp_dir = tempfile.mkdtemp()
snapshot_download(
    repo_id=HF_REPO_ID,
    repo_type="dataset",
    allow_patterns=f"{HF_FOLDER}/*",  # only the LDRRMF folder, nothing else
    local_dir=tmp_dir,
)

# Copy the Excel files into the volume
TARGET_DIR.mkdir(parents=True, exist_ok=True)
for src in sorted(Path(tmp_dir, HF_FOLDER).glob("*.xlsx")):
    shutil.copy2(src, TARGET_DIR / src.name)  # same file name, content untouched

shutil.rmtree(tmp_dir)  # remove the temporary download

# Show what landed
files = sorted(TARGET_DIR.glob("*.xlsx"))
for f in files:
    print(f.name, f.stat().st_size)
print(f"{len(files)} files in {TARGET_DIR} (expected 7: FY2018 to FY2024)")
