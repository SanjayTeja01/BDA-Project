import json
import os
from typing import Optional
from backend.logger import get_logger

logger = get_logger(__name__)

CATALOG_PATH = os.getenv("DATASETS_CONFIG_PATH", "backend/config/datasets.json")
_catalog: dict = {}


def load_catalog() -> dict:
    global _catalog
    if _catalog:
        return _catalog
    try:
        with open(CATALOG_PATH, "r") as f:
            data = json.load(f)
        _catalog = {d["dataset_name"]: d for d in data["datasets"]}
        logger.info(f"Loaded {len(_catalog)} datasets from catalog.")
        return _catalog
    except Exception as e:
        logger.error(f"Failed to load dataset catalog: {e}")
        raise


def list_datasets() -> list:
    catalog = load_catalog()
    return [
        {
            "dataset_name": v["dataset_name"],
            "category": v["category"],
            "description": v["description"],
            "estimated_size": v["estimated_size"],
            "format": v["format"],
            "version": v["version"],
            "last_updated": v["last_updated"],
            "primary_metric": v["primary_metric"],
            "time_column": v.get("time_column"),
            "source_url": v.get("source_url", ""),
        }
        for v in catalog.values()
    ]


def get_dataset_meta(name: str) -> Optional[dict]:
    catalog = load_catalog()
    return catalog.get(name)


def reload_catalog():
    global _catalog
    _catalog = {}
    return load_catalog()
