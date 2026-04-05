from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType, TimestampType, DateType, BooleanType
from backend.logger import get_logger

logger = get_logger(__name__)


def profile_dataset(df: DataFrame) -> dict:
    """Compute comprehensive data profile without altering the dataset."""
    logger.info("Starting data profiling...")

    row_count = df.count()
    col_count = len(df.columns)
    schema = df.schema

    numeric_cols, categorical_cols, date_cols, boolean_cols = [], [], [], []
    for field in schema.fields:
        if isinstance(field.dataType, NumericType):
            numeric_cols.append(field.name)
        elif isinstance(field.dataType, (TimestampType, DateType)):
            date_cols.append(field.name)
        elif isinstance(field.dataType, BooleanType):
            boolean_cols.append(field.name)
        else:
            categorical_cols.append(field.name)

    # Null analysis
    null_counts = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns
    ]).collect()[0].asDict()
    null_pct = {c: round(null_counts[c] / row_count * 100, 2) if row_count > 0 else 0.0 for c in df.columns}

    # Unique counts (sampled for performance on large datasets)
    sample_df = df.sample(fraction=min(1.0, 100000 / max(row_count, 1)), seed=42) if row_count > 100000 else df
    unique_counts = {}
    for c in df.columns:
        unique_counts[c] = sample_df.select(c).distinct().count()

    # High cardinality detection
    high_cardinality = [c for c in categorical_cols if unique_counts.get(c, 0) > 100]

    # Numeric summary stats
    numeric_stats = {}
    if numeric_cols:
        stats_rows = df.select(numeric_cols).summary("min", "max", "mean", "stddev", "25%", "50%", "75%").collect()
        stats_map = {row["summary"]: row.asDict() for row in stats_rows}
        for c in numeric_cols:
            try:
                numeric_stats[c] = {
                    "min": _safe_float(stats_map.get("min", {}).get(c)),
                    "max": _safe_float(stats_map.get("max", {}).get(c)),
                    "mean": _safe_float(stats_map.get("mean", {}).get(c)),
                    "stddev": _safe_float(stats_map.get("stddev", {}).get(c)),
                    "p25": _safe_float(stats_map.get("25%", {}).get(c)),
                    "median": _safe_float(stats_map.get("50%", {}).get(c)),
                    "p75": _safe_float(stats_map.get("75%", {}).get(c)),
                }
            except Exception:
                numeric_stats[c] = {}

    # Skew detection
    skew_flags = {}
    for c in numeric_cols:
        s = numeric_stats.get(c, {})
        mean = s.get("mean", 0) or 0
        median = s.get("median", 0) or 0
        stddev = s.get("stddev", 1) or 1
        if stddev > 0:
            skew_approx = (mean - median) / stddev
            skew_flags[c] = {
                "skew_approx": round(skew_approx, 4),
                "is_skewed": abs(skew_approx) > 0.5,
                "direction": "right" if skew_approx > 0.5 else ("left" if skew_approx < -0.5 else "symmetric"),
            }

    column_profiles = []
    for field in schema.fields:
        c = field.name
        column_profiles.append({
            "column": c,
            "dtype": str(field.dataType),
            "null_count": null_counts.get(c, 0),
            "null_pct": null_pct.get(c, 0.0),
            "unique_count": unique_counts.get(c, 0),
            "is_high_cardinality": c in high_cardinality,
            "column_type": (
                "numeric" if c in numeric_cols else
                "datetime" if c in date_cols else
                "boolean" if c in boolean_cols else
                "categorical"
            ),
            "numeric_stats": numeric_stats.get(c),
            "skew_info": skew_flags.get(c),
        })

    result = {
        "row_count": row_count,
        "column_count": col_count,
        "numeric_columns": numeric_cols,
        "categorical_columns": categorical_cols,
        "date_columns": date_cols,
        "boolean_columns": boolean_cols,
        "high_cardinality_columns": high_cardinality,
        "null_percentage": null_pct,
        "unique_counts": unique_counts,
        "column_profiles": column_profiles,
        "skew_flags": skew_flags,
        "duplicate_estimate": max(0, row_count - sample_df.distinct().count()) if row_count <= 500000 else "large_dataset_skipped",
    }

    logger.info(f"Profiling complete: {row_count} rows, {col_count} cols, {len(numeric_cols)} numeric, {len(categorical_cols)} categorical")
    return result


def _safe_float(val) -> float | None:
    try:
        return round(float(val), 4) if val is not None else None
    except (TypeError, ValueError):
        return None
