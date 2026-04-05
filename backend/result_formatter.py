import math
from datetime import datetime, timezone
from backend.logger import get_logger

logger = get_logger(__name__)


def _sanitize(obj):
    """
    Recursively walk any dict/list/float and replace values that are not
    JSON-compliant (NaN, Infinity, -Infinity) with None.
    Python's json.dumps raises ValueError on these; Firestore also rejects them.
    """
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    # int, str, bool, None — pass through unchanged
    return obj


def format_result(
    job_id: str,
    uid: str,
    meta: dict,
    profile: dict,
    cleaning_summary: dict,
    quality_score: dict,
    analytics: dict,
    execution_metrics: dict,
    insights: list,
    viz_recs: list,
    filters: list,
) -> dict:
    """Assemble, sanitize, and return the full structured result JSON."""
    result = {
        "dataset_info": {
            "dataset_name":   meta.get("dataset_name"),
            "category":       meta.get("category"),
            "description":    meta.get("description"),
            "format":         meta.get("format"),
            "estimated_size": meta.get("estimated_size"),
            "version":        meta.get("version"),
            "last_updated":   meta.get("last_updated"),
            "source_url":     meta.get("source_url", ""),
            "primary_metric": meta.get("primary_metric"),
            "time_column":    meta.get("time_column"),
        },
        "job_info": {
            "job_id":       job_id,
            "uid":          uid,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        },
        "profiling_report":           profile,
        "cleaning_summary":           cleaning_summary,
        "quality_score":              quality_score,
        "summary_metrics":            analytics.get("summary_metrics", {}),
        "category_analysis":          analytics.get("category_analysis", {}),
        "time_analysis":              analytics.get("time_analysis", {}),
        "distribution_analysis":      analytics.get("distribution_analysis", {}),
        "correlation_analysis":       analytics.get("correlation_analysis", {}),
        "execution_metrics":          execution_metrics,
        "insights":                   insights,
        "visualization_recommendations": viz_recs,
        "filter_spec":                filters,
    }

    # ── Sanitize ALL float values recursively before storing/returning ────────
    # Spark can produce NaN/Inf in std deviation, correlation, skew, etc.
    # json.dumps and Firestore both crash on these values.
    result = _sanitize(result)

    logger.info(f"Result formatted and sanitized for job {job_id}")
    return result