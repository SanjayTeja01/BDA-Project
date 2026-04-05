"""
SparkInsight — Real Dataset Setup Script
========================================
Downloads real, publicly available datasets for the SparkInsight Analytics Platform.
Run this ONCE before starting the backend.

Usage:
    python setup_datasets.py                  # download all datasets
    python setup_datasets.py --dataset nyc_taxi_trips   # download one
    python setup_datasets.py --list           # list available datasets

Requirements:
    pip install requests tqdm kaggle

Kaggle datasets require a Kaggle API key:
    1. Go to https://www.kaggle.com/account → Create New API Token
    2. Place kaggle.json in ~/.kaggle/kaggle.json (Linux/Mac)
       or C:\\Users\\<user>\\.kaggle\\kaggle.json (Windows)
    3. chmod 600 ~/.kaggle/kaggle.json

Direct-download datasets (no Kaggle key needed):
    - noaa_climate_data         (NOAA open data)
    - world_bank_indicators     (World Bank open data)
    - open_university_learning  (Open University open data)
"""

import os
import sys
import argparse
import zipfile
import shutil
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("setup_datasets")

# ── Dataset download specs ───────────────────────────────────────────────────
DATASETS = {
    # ── Direct HTTP downloads (no auth needed) ───────────────────────────────
    "noaa_climate_data": {
        "method": "direct",
        "description": "NOAA Global Summary of the Day (~200MB)",
        "target_dir": "datasets/climate",
        "target_file": "datasets/climate/noaa_climate_data.csv",
        # GSOD data via NOAA's public S3 bucket (2022 sample, ~200MB)
        "url": "https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive/2022.tar.gz",
        "extract": "tar_merge_csv",
        "notes": "Downloads 2022 GSOD data. For multi-year data, re-run with different years.",
    },
    "world_bank_indicators": {
        "method": "direct",
        "description": "World Bank Development Indicators (~180MB)",
        "target_dir": "datasets/public_data",
        "target_file": "datasets/public_data/world_bank_indicators.csv",
        "url": "https://databank.worldbank.org/data/download/WDI_CSV.zip",
        "extract": "zip_rename",
        "zip_source_file": "WDIData.csv",
        "notes": "Official World Bank open data, no auth required.",
    },
    "open_university_learning": {
        "method": "direct_multi",
        "description": "Open University Learning Analytics (~100MB)",
        "target_dir": "datasets/education",
        "target_file": "datasets/education/open_university_learning.csv",
        "urls": [
            ("https://analyse.kmi.open.ac.uk/open_dataset/download/studentAssessment.csv", "studentAssessment.csv"),
            ("https://analyse.kmi.open.ac.uk/open_dataset/download/studentInfo.csv", "studentInfo.csv"),
            ("https://analyse.kmi.open.ac.uk/open_dataset/download/assessments.csv", "assessments.csv"),
        ],
        "merge_key": "id_student",
        "notes": "Merges studentAssessment + studentInfo + assessments CSVs.",
    },

    # ── Kaggle downloads (require kaggle.json) ───────────────────────────────
    "nyc_taxi_trips": {
        "method": "kaggle",
        "description": "NYC Yellow Taxi Trip Records (~150MB sample)",
        "target_dir": "datasets/transportation",
        "target_file": "datasets/transportation/nyc_taxi_trips.csv",
        "kaggle_dataset": "elemento/nyc-yellow-taxi-trip-data",
        "kaggle_file": "yellow_tripdata_2015-01.csv",
        "notes": "Jan 2015 sample. Full data at https://www.nyc.gov/site/tlc/",
    },
    "global_stock_market": {
        "method": "kaggle",
        "description": "S&P 500 Stock Data (~290MB)",
        "target_dir": "datasets/finance",
        "target_file": "datasets/finance/global_stock_market.csv",
        "kaggle_dataset": "camnugent/sandp500",
        "kaggle_file": "all_stocks_5yr.csv",
        "notes": "5-year daily OHLCV for all S&P 500 stocks.",
    },
    "amazon_product_reviews": {
        "method": "kaggle",
        "description": "Amazon Fine Food Reviews (~301MB)",
        "target_dir": "datasets/ecommerce",
        "target_file": "datasets/ecommerce/amazon_product_reviews.csv",
        "kaggle_dataset": "snap/amazon-fine-food-reviews",
        "kaggle_file": "Reviews.csv",
        "notes": "568K real product reviews from Amazon.",
    },
    "chicago_crimes": {
        "method": "kaggle",
        "description": "Chicago Crimes 2001-Present (~1.7GB)",
        "target_dir": "datasets/public_data",
        "target_file": "datasets/public_data/chicago_crimes.csv",
        "kaggle_dataset": "chicago/chicago-crime",
        "kaggle_file": "chicago-crime.csv",
        "notes": "8M+ real crime incident reports.",
    },
    "us_accidents": {
        "method": "kaggle",
        "description": "US Accidents 2016-2023 (~1.1GB)",
        "target_dir": "datasets/transportation",
        "target_file": "datasets/transportation/us_accidents.csv",
        "kaggle_dataset": "sobhanmoosavi/us-accidents",
        "kaggle_file": "US_Accidents_March23.csv",
        "notes": "7.7M real traffic accident records.",
    },
    "flight_delays": {
        "method": "kaggle",
        "description": "US Flight Delays (~500MB)",
        "target_dir": "datasets/transportation",
        "target_file": "datasets/transportation/flight_delays.csv",
        "kaggle_dataset": "yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018",
        "kaggle_file": "2018.csv",
        "notes": "US DOT on-time flight performance data.",
    },
    "stackoverflow_survey": {
        "method": "kaggle",
        "description": "Stack Overflow Developer Survey 2023 (~90MB)",
        "target_dir": "datasets/social_media",
        "target_file": "datasets/social_media/stackoverflow_survey.csv",
        "kaggle_dataset": "stackoverflow/stack-overflow-2023-developers-survey",
        "kaggle_file": "survey_results_public.csv",
        "notes": "90K+ real developer survey responses.",
    },
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def download_file(url: str, dest_path: str):
    """Download with progress bar."""
    try:
        import requests
        from tqdm import tqdm
    except ImportError:
        logger.error("Missing dependencies. Run: pip install requests tqdm")
        sys.exit(1)

    logger.info(f"Downloading: {url}")
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))

    with open(dest_path, "wb") as f, tqdm(
        desc=os.path.basename(dest_path),
        total=total, unit="iB", unit_scale=True, unit_divisor=1024
    ) as bar:
        for chunk in resp.iter_content(chunk_size=8192):
            size = f.write(chunk)
            bar.update(size)
    logger.info(f"Saved to: {dest_path}")


def extract_zip(zip_path: str, extract_to: str, source_file: str, target_file: str):
    """Extract a specific file from a ZIP and rename it."""
    logger.info(f"Extracting {source_file} from {zip_path}...")
    with zipfile.ZipFile(zip_path, "r") as z:
        names = z.namelist()
        match = next((n for n in names if os.path.basename(n) == source_file), None)
        if not match:
            logger.warning(f"{source_file} not found in ZIP. Available: {names[:10]}")
            # Try the first CSV
            match = next((n for n in names if n.endswith(".csv")), None)
        if match:
            z.extract(match, extract_to)
            extracted = os.path.join(extract_to, match)
            shutil.move(extracted, target_file)
            logger.info(f"Extracted to: {target_file}")
        else:
            logger.error("No CSV found in ZIP archive.")
    os.remove(zip_path)


def extract_tar_merge_csv(tar_path: str, extract_to: str, target_file: str):
    """Extract tar.gz of CSVs and merge them into one file."""
    import tarfile
    import glob

    tmp_dir = os.path.join(extract_to, "_tmp_extract")
    os.makedirs(tmp_dir, exist_ok=True)

    logger.info(f"Extracting tar: {tar_path}...")
    with tarfile.open(tar_path, "r:gz") as t:
        t.extractall(tmp_dir)

    csv_files = glob.glob(os.path.join(tmp_dir, "**", "*.csv"), recursive=True)
    logger.info(f"Found {len(csv_files)} CSV files. Merging...")

    header_written = False
    with open(target_file, "w", encoding="utf-8") as out:
        for i, cf in enumerate(sorted(csv_files)[:500]):  # cap at 500 files
            with open(cf, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
                if not lines:
                    continue
                if not header_written:
                    out.writelines(lines)
                    header_written = True
                else:
                    out.writelines(lines[1:])  # skip header on subsequent files

    shutil.rmtree(tmp_dir)
    os.remove(tar_path)
    logger.info(f"Merged CSV saved to: {target_file}")


def download_direct(name: str, spec: dict):
    """Download a dataset via direct HTTP."""
    ensure_dir(spec["target_dir"])
    target = spec["target_file"]

    if os.path.exists(target):
        logger.info(f"✅ {name} already exists at {target}. Skipping.")
        return

    extract = spec.get("extract", "")

    if extract == "zip_rename":
        tmp_zip = os.path.join(spec["target_dir"], "_download.zip")
        download_file(spec["url"], tmp_zip)
        extract_zip(tmp_zip, spec["target_dir"], spec.get("zip_source_file", ""), target)

    elif extract == "tar_merge_csv":
        tmp_tar = os.path.join(spec["target_dir"], "_download.tar.gz")
        download_file(spec["url"], tmp_tar)
        extract_tar_merge_csv(tmp_tar, spec["target_dir"], target)

    else:
        download_file(spec["url"], target)

    logger.info(f"✅ {name} ready.")


def download_direct_multi(name: str, spec: dict):
    """Download multiple files and merge them (OULAD style)."""
    import csv

    ensure_dir(spec["target_dir"])
    target = spec["target_file"]

    if os.path.exists(target):
        logger.info(f"✅ {name} already exists at {target}. Skipping.")
        return

    tmp_files = []
    for url, fname in spec["urls"]:
        tmp = os.path.join(spec["target_dir"], fname)
        if not os.path.exists(tmp):
            download_file(url, tmp)
        tmp_files.append(tmp)

    # Simple concatenation: write all files, deduplicating the header
    logger.info(f"Merging {len(tmp_files)} files into {target}...")
    header_written = False
    with open(target, "w", newline="", encoding="utf-8") as out_f:
        writer = None
        for tmp in tmp_files:
            with open(tmp, "r", encoding="utf-8", errors="replace") as in_f:
                reader = csv.reader(in_f)
                rows = list(reader)
                if not rows:
                    continue
                if not header_written:
                    writer = csv.writer(out_f)
                    writer.writerows(rows)
                    header_written = True
                else:
                    writer.writerows(rows[1:])

    for tmp in tmp_files:
        os.remove(tmp)

    logger.info(f"✅ {name} ready.")


def download_kaggle(name: str, spec: dict):
    """Download a dataset via Kaggle API."""
    try:
        from kaggle.api.kaggle_api_extended import KaggleApiClient
        import kaggle
    except ImportError:
        logger.error("kaggle package not installed. Run: pip install kaggle")
        return

    ensure_dir(spec["target_dir"])
    target = spec["target_file"]

    if os.path.exists(target):
        logger.info(f"✅ {name} already exists at {target}. Skipping.")
        return

    try:
        api = kaggle.api
        api.authenticate()
    except Exception as e:
        logger.error(
            f"Kaggle auth failed for '{name}': {e}\n"
            "Place kaggle.json in ~/.kaggle/ and run: chmod 600 ~/.kaggle/kaggle.json"
        )
        logger.warning(f"⚠️  Skipping {name}. The backend will use synthetic data instead.")
        return

    logger.info(f"Downloading Kaggle dataset: {spec['kaggle_dataset']}")
    try:
        api.dataset_download_file(
            spec["kaggle_dataset"],
            file_name=spec["kaggle_file"],
            path=spec["target_dir"],
            force=False,
            quiet=False,
        )
        # Kaggle downloads as .zip if compressed
        downloaded_zip = os.path.join(spec["target_dir"], spec["kaggle_file"] + ".zip")
        if os.path.exists(downloaded_zip):
            extract_zip(downloaded_zip, spec["target_dir"], spec["kaggle_file"], target)
        else:
            downloaded = os.path.join(spec["target_dir"], spec["kaggle_file"])
            if downloaded != target:
                shutil.move(downloaded, target)
        logger.info(f"✅ {name} ready.")
    except Exception as e:
        logger.error(f"Kaggle download failed for '{name}': {e}")
        logger.warning(f"⚠️  Skipping {name}. The backend will use synthetic data instead.")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(dataset_names: list):
    for name in dataset_names:
        if name not in DATASETS:
            logger.warning(f"Unknown dataset: {name}. Run with --list to see options.")
            continue

        spec = DATASETS[name]
        logger.info(f"\n{'='*60}")
        logger.info(f"Setting up: {name}")
        logger.info(f"Description: {spec['description']}")
        logger.info(f"Notes: {spec.get('notes','')}")
        logger.info(f"{'='*60}")

        method = spec["method"]
        if method == "direct":
            download_direct(name, spec)
        elif method == "direct_multi":
            download_direct_multi(name, spec)
        elif method == "kaggle":
            download_kaggle(name, spec)
        else:
            logger.warning(f"Unknown method '{method}' for {name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SparkInsight dataset setup")
    parser.add_argument("--dataset", help="Download a specific dataset by name")
    parser.add_argument("--list", action="store_true", help="List all available datasets")
    parser.add_argument("--direct-only", action="store_true",
                        help="Only download datasets that don't require Kaggle API")
    args = parser.parse_args()

    if args.list:
        print("\nAvailable datasets:\n")
        for name, spec in DATASETS.items():
            method_label = "🔓 Direct" if spec["method"].startswith("direct") else "🔑 Kaggle API"
            print(f"  {method_label}  {name}")
            print(f"           {spec['description']}")
            print(f"           target: {spec['target_file']}\n")
        sys.exit(0)

    if args.dataset:
        run([args.dataset])
    elif args.direct_only:
        names = [n for n, s in DATASETS.items() if s["method"].startswith("direct")]
        logger.info(f"Downloading {len(names)} direct datasets (no Kaggle key needed)...")
        run(names)
    else:
        logger.info(f"Downloading all {len(DATASETS)} datasets...")
        run(list(DATASETS.keys()))

    logger.info("\n✅ Dataset setup complete!")
    logger.info("Note: Any skipped datasets will use synthetic data automatically.")
    logger.info("Start the backend with: uvicorn backend.main:app --reload")
