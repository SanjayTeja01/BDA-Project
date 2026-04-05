"""
Heuristic-based adaptive visualization recommendation engine.
All logic is rule-driven and explainable. No static hardcoded mappings.
"""
from backend.logger import get_logger

logger = get_logger(__name__)

CHART_TYPES = {
    "bar": "Bar Chart",
    "line": "Line Chart",
    "pie": "Pie Chart",
    "histogram": "Histogram",
    "scatter": "Scatter Plot",
    "multi_line": "Multi-Line Chart",
    "heatmap": "Correlation Heatmap",
    "area": "Area Chart",
}


def recommend_visualizations(analytics_result: dict, profile: dict, meta: dict) -> list:
    """
    Deterministic heuristic rules to recommend chart types for each analytic section.
    Returns list of {chart_id, chart_type, title, data_source, config, reasoning}.
    """
    recs = []
    numeric_cols = profile.get("numeric_columns", [])
    cat_cols = profile.get("categorical_columns", [])
    date_cols = profile.get("date_columns", [])
    skew_flags = profile.get("skew_flags", {})
    primary_metric = meta.get("primary_metric", "")

    # --- Rule 1: Category Analysis ---
    cat = analytics_result.get("category_analysis", {})
    if cat.get("categories"):
        n_cats = len(cat["categories"])
        if n_cats <= 6:
            chart_type = "pie"
            reasoning = f"Pie chart chosen: ≤6 categories ({n_cats}), ideal for part-to-whole composition."
        elif n_cats <= 15:
            chart_type = "bar"
            reasoning = f"Bar chart chosen: {n_cats} categories, good for direct comparison across groups."
        else:
            chart_type = "bar"
            reasoning = f"Horizontal bar chart chosen: {n_cats} categories, horizontal layout aids readability."

        recs.append({
            "chart_id": "cat_distribution",
            "chart_type": chart_type,
            "title": f"{cat.get('column', 'Category')} Distribution",
            "data_source": "category_analysis",
            "config": {
                "x_field": "category",
                "y_field": "count",
                "color_by": "category",
                "show_percentage": True,
            },
            "reasoning": reasoning,
        })

        # Secondary: metric by category (if primary metric available)
        if cat["categories"] and "total" in cat["categories"][0]:
            recs.append({
                "chart_id": "cat_metric",
                "chart_type": "bar",
                "title": f"{primary_metric} by {cat.get('column', 'Category')}",
                "data_source": "category_analysis",
                "config": {"x_field": "category", "y_field": "total", "sort": "desc"},
                "reasoning": "Bar chart chosen: comparing aggregate metric across categories.",
            })

    # --- Rule 2: Time Analysis ---
    time = analytics_result.get("time_analysis", {})
    if time.get("available"):
        monthly = time.get("monthly", [])
        growth = time.get("overall_growth_rate_pct")

        if len(monthly) > 24:
            chart_type = "area"
            reasoning = "Area chart chosen: >24 time points, area fill emphasizes volume over time."
        elif growth and growth > 10:
            chart_type = "line"
            reasoning = f"Line chart chosen: positive growth trend (+{growth}%), line chart highlights trend direction."
        else:
            chart_type = "line"
            reasoning = "Line chart chosen: time-series data, line best represents temporal continuity."

        recs.append({
            "chart_id": "time_series",
            "chart_type": chart_type,
            "title": f"{primary_metric} Over Time (Monthly)",
            "data_source": "time_analysis.monthly",
            "config": {"x_field": "period", "y_field": "total", "smooth": True},
            "reasoning": reasoning,
        })

        recs.append({
            "chart_id": "yearly_trend",
            "chart_type": "bar",
            "title": f"Yearly {primary_metric} Trend",
            "data_source": "time_analysis.yearly",
            "config": {"x_field": "year", "y_field": "total"},
            "reasoning": "Bar chart chosen: yearly aggregation has few points, bar chart suits discrete yearly comparison.",
        })

    # --- Rule 3: Distribution Analysis ---
    dist = analytics_result.get("distribution_analysis", {})
    if dist.get("available"):
        skew_info = skew_flags.get(primary_metric, {})
        is_skewed = skew_info.get("is_skewed", False)

        recs.append({
            "chart_id": "histogram",
            "chart_type": "histogram",
            "title": f"{primary_metric} Distribution",
            "data_source": "distribution_analysis.histogram_bins",
            "config": {
                "x_field": "range_start",
                "y_field": "count",
                "bins": 20,
                "log_scale": is_skewed,
            },
            "reasoning": (
                "Histogram with log scale: distribution is right-skewed, log scale reveals true shape."
                if is_skewed else
                "Histogram chosen: best for visualizing frequency distribution of continuous numeric data."
            ),
        })

    # --- Rule 4: Correlation Analysis ---
    corr = analytics_result.get("correlation_analysis", {})
    if corr.get("available") and len(corr.get("columns", [])) >= 2:
        strong = corr.get("strong_correlations", [])
        if strong:
            # Scatter for strongest pair
            top = strong[0]
            recs.append({
                "chart_id": "scatter_correlation",
                "chart_type": "scatter",
                "title": f"Correlation: {top['col1']} vs {top['col2']}",
                "data_source": "correlation_analysis",
                "config": {"x_field": top["col1"], "y_field": top["col2"]},
                "reasoning": (
                    f"Scatter plot chosen: strong {top['direction']} correlation ({top['correlation']}) "
                    f"between {top['col1']} and {top['col2']}."
                ),
            })

        recs.append({
            "chart_id": "correlation_heatmap",
            "chart_type": "heatmap",
            "title": "Correlation Matrix",
            "data_source": "correlation_analysis.matrix",
            "config": {"columns": corr.get("columns", [])},
            "reasoning": "Heatmap chosen: multiple numeric columns, heatmap best displays pairwise correlation strength.",
        })

    # --- Rule 5: Multi-metric multi-line (if multiple numeric cols + time) ---
    if len(numeric_cols) >= 3 and time.get("available"):
        recs.append({
            "chart_id": "multi_metric_time",
            "chart_type": "multi_line",
            "title": "Multi-Metric Trend Comparison",
            "data_source": "time_analysis.monthly",
            "config": {"x_field": "period", "y_fields": numeric_cols[:4]},
            "reasoning": "Multi-line chart chosen: multiple numeric metrics over time, line chart supports comparison.",
        })

    logger.info(f"Visualization engine recommended {len(recs)} charts.")
    return recs
