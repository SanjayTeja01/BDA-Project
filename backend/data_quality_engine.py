import os
import json
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType
from backend.logger import get_logger

logger = get_logger(__name__)

DEFAULT_WEIGHTS = {
    "completeness": 0.30,
    "uniqueness": 0.20,
    "validity": 0.20,
    "consistency": 0.15,
    "outlier_impact": 0.10,
    "skew_severity": 0.05,
}

QUALITY_CONFIG_PATH = os.getenv("QUALITY_WEIGHTS_PATH", "backend/config/quality_weights.json")


def _load_weights() -> dict:
    if os.path.exists(QUALITY_CONFIG_PATH):
        try:
            with open(QUALITY_CONFIG_PATH) as f:
                return json.load(f)
        except Exception:
            pass
    return DEFAULT_WEIGHTS


def compute_quality_score(df: DataFrame, profile: dict, meta: dict) -> dict:
    """Compute adaptive weighted data quality score (0-100)."""
    logger.info("Computing data quality score...")
    weights = _load_weights()
    total_rows = profile["row_count"]
    total_cols = profile["column_count"]

    # --- Completeness ---
    avg_null_pct = (sum(profile["null_percentage"].values()) / max(total_cols, 1))
    completeness = max(0.0, 100.0 - avg_null_pct)

    # --- Uniqueness ---
    numeric_cols = profile["numeric_columns"]
    cat_cols = profile["categorical_columns"]
    unique_ratios = []
    for c, uc in profile["unique_counts"].items():
        if total_rows > 0:
            unique_ratios.append(min(1.0, uc / total_rows))
    uniqueness = (sum(unique_ratios) / max(len(unique_ratios), 1)) * 100 if unique_ratios else 100.0

    # --- Validity: check for outliers in numeric cols ---
    outlier_scores = []
    for c in numeric_cols[:8]:  # cap for performance
        try:
            s = next((p for p in profile["column_profiles"] if p["column"] == c), None)
            if s and s.get("numeric_stats"):
                ns = s["numeric_stats"]
                iqr = (ns.get("p75") or 0) - (ns.get("p25") or 0)
                if iqr > 0:
                    lower = (ns.get("p25") or 0) - 1.5 * iqr
                    upper = (ns.get("p75") or 0) + 1.5 * iqr
                    outlier_count = df.filter((F.col(c) < lower) | (F.col(c) > upper)).count()
                    outlier_pct = outlier_count / max(total_rows, 1) * 100
                    outlier_scores.append(max(0, 100 - outlier_pct * 10))
                else:
                    outlier_scores.append(100.0)
        except Exception:
            outlier_scores.append(90.0)
    validity = sum(outlier_scores) / max(len(outlier_scores), 1) if outlier_scores else 95.0
    outlier_impact = 100.0 - validity

    # --- Consistency: std dev uniformity across numeric cols ---
    consistency_scores = []
    for p in profile["column_profiles"]:
        if p.get("numeric_stats") and p["numeric_stats"].get("mean"):
            mean = p["numeric_stats"]["mean"] or 1
            std = p["numeric_stats"].get("stddev") or 0
            cv = std / abs(mean) if mean != 0 else 0
            consistency_scores.append(max(0, 100 - min(cv * 20, 100)))
    consistency = sum(consistency_scores) / max(len(consistency_scores), 1) if consistency_scores else 90.0

    # --- Skew Severity ---
    skew_penalties = []
    for c, sk in profile.get("skew_flags", {}).items():
        sa = abs(sk.get("skew_approx", 0))
        skew_penalties.append(min(sa * 10, 100))
    skew_penalty = sum(skew_penalties) / max(len(skew_penalties), 1) if skew_penalties else 0.0
    skew_severity_score = max(0, 100 - skew_penalty)

    components = {
        "completeness": round(completeness, 2),
        "uniqueness": round(uniqueness, 2),
        "validity": round(validity, 2),
        "consistency": round(consistency, 2),
        "outlier_impact": round(max(0, 100 - outlier_impact), 2),
        "skew_severity": round(skew_severity_score, 2),
    }

    overall = sum(components[k] * weights[k] for k in weights if k in components)
    overall = round(min(100.0, max(0.0, overall)), 2)

    # Recommendations
    recommendations = []
    if completeness < 80:
        recommendations.append("High null values detected. Consider imputation or column removal strategies.")
    if uniqueness < 50:
        recommendations.append("Low uniqueness ratio. Check for duplicate records or repeated categorical patterns.")
    if validity < 85:
        recommendations.append("Significant outliers detected. Review and apply appropriate capping or transformation.")
    if consistency < 75:
        recommendations.append("High coefficient of variation in numeric columns. Normalize or standardize before analysis.")
    if skew_severity_score < 70:
        recommendations.append("Skewed distributions found. Consider log transformation for affected numeric columns.")
    if not recommendations:
        recommendations.append("Dataset quality is good. No major issues detected.")

    result = {
        "overall_score": overall,
        "grade": _grade(overall),
        "component_breakdown": components,
        "weights_used": weights,
        "quality_recommendations": recommendations,
    }

    logger.info(f"Quality score: {overall}/100 ({result['grade']})")
    return result


def _grade(score: float) -> str:
    if score >= 90: return "A"
    if score >= 80: return "B"
    if score >= 70: return "C"
    if score >= 60: return "D"
    return "F"
