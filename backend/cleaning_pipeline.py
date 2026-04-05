import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType, TimestampType, DateType
from backend.logger import get_logger

logger = get_logger(__name__)
NULL_THRESHOLD = 0.80  # Drop columns with > 80% nulls


def clean_dataset(df: DataFrame, meta: dict, profile: dict) -> tuple[DataFrame, dict]:
    """Apply full cleaning pipeline. Returns (cleaned_df, cleaning_summary)."""
    logger.info(f"Starting cleaning pipeline for '{meta['dataset_name']}'...")

    stats = {
        "initial_rows": profile["row_count"],
        "initial_columns": profile["column_count"],
        "steps": [],
    }

    # Step 1: Normalize column names
    renamed = {c: _normalize_col(c) for c in df.columns}
    df = df.toDF(*[renamed[c] for c in df.columns])
    stats["steps"].append({"step": "normalize_column_names", "renames": sum(1 for k, v in renamed.items() if k != v)})

    # Step 2: Drop high-null columns
    drop_cols = [renamed.get(c, c) for c, pct in profile["null_percentage"].items() if pct > NULL_THRESHOLD * 100]
    if drop_cols:
        df = df.drop(*drop_cols)
        stats["steps"].append({"step": "drop_high_null_columns", "dropped": drop_cols})
        logger.info(f"Dropped high-null columns: {drop_cols}")

    # Step 3: Remove duplicates
    before_dedup = df.count()
    df = df.dropDuplicates()
    after_dedup = df.count()
    dupes_removed = before_dedup - after_dedup
    stats["steps"].append({"step": "remove_duplicates", "duplicates_removed": dupes_removed})

    # Step 4: Fill numeric nulls with mean
    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
    if numeric_cols:
        means = df.select([F.mean(c).alias(c) for c in numeric_cols]).collect()[0].asDict()
        fill_map = {c: round(means[c], 4) for c in numeric_cols if means.get(c) is not None}
        df = df.fillna(fill_map)
        stats["steps"].append({"step": "fill_numeric_nulls_mean", "columns_filled": list(fill_map.keys())})

    # Step 5: Fill categorical nulls with "Unknown"
    cat_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    if cat_cols:
        df = df.fillna("Unknown", subset=cat_cols)
        stats["steps"].append({"step": "fill_categorical_nulls", "value": "Unknown", "columns": cat_cols})

    # Step 6: Remove invalid negatives
    if not meta.get("allowed_negative_values", True):
        for c in numeric_cols:
            if c in df.columns:
                df = df.filter(F.col(c) >= 0)
        stats["steps"].append({"step": "remove_invalid_negatives", "columns_checked": numeric_cols})

    # Step 7: Trim strings
    for c in cat_cols:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))
    stats["steps"].append({"step": "trim_strings", "columns": len(cat_cols)})

    # Step 8: Cache cleaned dataset
    df = df.cache()
    df.count()  # Force cache

    stats["final_rows"] = df.count()
    stats["final_columns"] = len(df.columns)
    stats["rows_removed"] = stats["initial_rows"] - stats["final_rows"]
    stats["columns_removed"] = stats["initial_columns"] - stats["final_columns"]
    logger.info(f"Cleaning complete. {stats['rows_removed']} rows removed, {stats['columns_removed']} cols removed.")
    return df, stats


def _normalize_col(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^\w]", "_", name)
    name = re.sub(r"__+", "_", name)
    name = name.strip("_")
    return name
