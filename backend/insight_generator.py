"""
Structured insight generator — heuristic rule-based, deterministic, schema-aware.
"""
from backend.logger import get_logger

logger = get_logger(__name__)


def generate_insights(analytics: dict, quality: dict, profile: dict, meta: dict) -> list:
    insights = []
    dataset_name = meta.get("dataset_name", "dataset")
    primary_metric = meta.get("primary_metric", "value")

    # Insight 1: Data quality
    score = quality.get("overall_score", 0)
    grade = quality.get("grade", "?")
    insights.append({
        "type": "quality",
        "severity": "info" if score >= 70 else "warning",
        "title": f"Data Quality Score: {score}/100 (Grade {grade})",
        "detail": (
            f"The dataset scored {score}/100 on the adaptive quality assessment. "
            + (", ".join(quality.get("quality_recommendations", [])))
        ),
    })

    # Insight 2: Volume
    total = analytics.get("summary_metrics", {}).get("total_records", 0)
    insights.append({
        "type": "volume",
        "severity": "info",
        "title": f"Dataset contains {total:,} records",
        "detail": f"Processed {total:,} records from '{dataset_name}' with {profile.get('column_count', 0)} columns.",
    })

    # Insight 3: Primary metric summary
    sm = analytics.get("summary_metrics", {})
    if sm.get("total") is not None:
        avg = sm.get("average", 0) or 0
        total_val = sm.get("total", 0) or 0
        insights.append({
            "type": "metric_summary",
            "severity": "info",
            "title": f"Total {primary_metric}: {total_val:,.2f}",
            "detail": f"Average {primary_metric} is {avg:,.2f}. Min: {sm.get('minimum', 0):,.2f}, Max: {sm.get('maximum', 0):,.2f}.",
        })

    # Insight 4: Top category
    cat = analytics.get("category_analysis", {})
    if cat.get("categories"):
        top = cat["categories"][0]
        insights.append({
            "type": "category",
            "severity": "info",
            "title": f"Top category: '{top['category']}' ({top['percentage']}%)",
            "detail": (
                f"'{top['category']}' leads with {top['count']:,} records ({top['percentage']}% of total). "
                f"Dataset has {cat.get('total_categories', 0)} unique categories."
            ),
        })

    # Insight 5: Growth trend
    time_a = analytics.get("time_analysis", {})
    if time_a.get("available") and time_a.get("overall_growth_rate_pct") is not None:
        g = time_a["overall_growth_rate_pct"]
        direction = "grown" if g > 0 else "declined"
        sev = "success" if g > 0 else "warning"
        insights.append({
            "type": "trend",
            "severity": sev,
            "title": f"{primary_metric} has {direction} by {abs(g):.1f}% over the analysis period",
            "detail": (
                f"Year-over-year comparison shows a {abs(g):.1f}% {'increase' if g > 0 else 'decrease'}. "
                + (f"Peak was in {time_a['peak_period']['period']} with {time_a['peak_period']['total']:,.2f}."
                   if time_a.get("peak_period") else "")
            ),
        })

    # Insight 6: Skew
    skews = profile.get("skew_flags", {})
    skewed = [(c, s) for c, s in skews.items() if s.get("is_skewed")]
    if skewed:
        names = ", ".join(f"{c} ({s['direction']})" for c, s in skewed[:3])
        insights.append({
            "type": "distribution",
            "severity": "warning",
            "title": f"{len(skewed)} column(s) show skewed distribution",
            "detail": f"Columns with significant skew: {names}. Consider log transformation before modeling.",
        })

    # Insight 7: Strong correlations
    corr_a = analytics.get("correlation_analysis", {})
    strong = corr_a.get("strong_correlations", [])
    if strong:
        top_corr = strong[0]
        insights.append({
            "type": "correlation",
            "severity": "info",
            "title": f"Strong {top_corr['direction']} correlation detected",
            "detail": (
                f"'{top_corr['col1']}' and '{top_corr['col2']}' have a "
                f"{top_corr['strength']} {top_corr['direction']} correlation (r={top_corr['correlation']})."
            ),
        })

    # Insight 8: Null columns warning
    high_nulls = [(c, p) for c, p in profile.get("null_percentage", {}).items() if p > 30]
    if high_nulls:
        names = ", ".join(f"{c} ({p}%)" for c, p in high_nulls[:3])
        insights.append({
            "type": "data_quality",
            "severity": "warning",
            "title": f"{len(high_nulls)} column(s) have >30% missing values",
            "detail": f"High null columns: {names}. Data completeness is reduced.",
        })

    logger.info(f"Generated {len(insights)} insights.")
    return insights
