"""
Adaptive filter engine — generates dynamic filter specs from schema.
Only re-runs aggregation on cached cleaned DataFrame, never profiling or cleaning.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, TimestampType, DateType, BooleanType, StringType
from backend.logger import get_logger

logger = get_logger(__name__)
MAX_CATEGORICAL_OPTIONS = 50
HIGH_CARDINALITY_THRESHOLD = 100


def generate_filter_spec(df: DataFrame, profile: dict) -> list:
    """Inspect cleaned DataFrame schema and return filter UI specs."""
    filters = []
    col_map = {f.name: f.dataType for f in df.schema.fields}

    for col, dtype in col_map.items():
        col_profile = next((p for p in profile["column_profiles"] if p["column"] == col), {})
        unique_count = col_profile.get("unique_count", 0)

        if isinstance(dtype, (TimestampType, DateType)):
            filters.append({
                "column": col,
                "filter_type": "date_range",
                "label": _label(col),
                "min_date": _get_min_date(df, col),
                "max_date": _get_max_date(df, col),
            })

        elif isinstance(dtype, NumericType):
            stats = col_profile.get("numeric_stats", {}) or {}
            filters.append({
                "column": col,
                "filter_type": "range_slider",
                "label": _label(col),
                "min": stats.get("min"),
                "max": stats.get("max"),
                "step": _compute_step(stats.get("min"), stats.get("max")),
            })

        elif isinstance(dtype, BooleanType):
            filters.append({
                "column": col,
                "filter_type": "toggle",
                "label": _label(col),
            })

        elif isinstance(dtype, StringType):
            if unique_count > HIGH_CARDINALITY_THRESHOLD:
                filters.append({
                    "column": col,
                    "filter_type": "search",
                    "label": _label(col),
                    "placeholder": f"Search {_label(col)}...",
                })
            elif unique_count <= MAX_CATEGORICAL_OPTIONS:
                options = [r[col] for r in df.select(col).distinct().orderBy(col).limit(MAX_CATEGORICAL_OPTIONS).collect()]
                filters.append({
                    "column": col,
                    "filter_type": "multi_select",
                    "label": _label(col),
                    "options": [str(o) for o in options if o is not None],
                })

    logger.info(f"Generated {len(filters)} dynamic filters from schema.")
    return filters


def _label(col: str) -> str:
    return col.replace("_", " ").title()


def _get_min_date(df: DataFrame, col: str) -> str | None:
    try:
        val = df.agg(F.min(col).alias("v")).collect()[0]["v"]
        return str(val) if val else None
    except Exception:
        return None


def _get_max_date(df: DataFrame, col: str) -> str | None:
    try:
        val = df.agg(F.max(col).alias("v")).collect()[0]["v"]
        return str(val) if val else None
    except Exception:
        return None


def _compute_step(mn, mx) -> float:
    try:
        span = float(mx) - float(mn)
        if span <= 0:
            return 1.0
        magnitude = 10 ** (len(str(int(span))) - 2)
        return max(0.01, round(span / 100, 4))
    except Exception:
        return 1.0
