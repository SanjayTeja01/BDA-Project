from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, TimestampType, DateType
from backend.logger import get_logger

logger = get_logger(__name__)

MAX_CATEGORY_VALUES = 20
HISTOGRAM_BINS = 20


def run_analytics(df: DataFrame, meta: dict, profile: dict, filters: dict = None) -> dict:
    """Run full distributed analytics pipeline using Spark DataFrame API."""
    logger.info(f"Starting analytics for dataset '{meta['dataset_name']}'...")

    if filters:
        df = _apply_filters(df, filters, profile)

    primary_metric = _find_metric_col(df, meta.get("primary_metric", ""))
    time_col = _find_col(df, meta.get("time_column", ""))
    category_col = _find_col(df, meta.get("category_column", ""))

    results = {}
    results["summary_metrics"] = _summary_metrics(df, primary_metric)
    results["category_analysis"] = _category_analysis(df, primary_metric, category_col)
    results["time_analysis"] = _time_analysis(df, primary_metric, time_col)
    results["distribution_analysis"] = _distribution_analysis(df, primary_metric, profile)
    results["correlation_analysis"] = _correlation_analysis(df, profile)

    logger.info("Analytics complete.")
    return results


def _find_metric_col(df: DataFrame, name: str) -> str | None:
    cols = [f.name for f in df.schema.fields]
    if name in cols:
        return name
    numeric = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
    return numeric[0] if numeric else None


def _find_col(df: DataFrame, name: str) -> str | None:
    if not name:
        return None
    cols = [f.name for f in df.schema.fields]
    if name in cols:
        return name
    # fuzzy match normalized
    norm = name.lower().replace(" ", "_")
    for c in cols:
        if c.lower().replace(" ", "_") == norm:
            return c
    return None


def _apply_filters(df: DataFrame, filters: dict, profile: dict) -> DataFrame:
    for col_name, filter_val in filters.items():
        if col_name not in df.columns:
            continue
        if isinstance(filter_val, dict):
            if "min" in filter_val and filter_val["min"] is not None:
                df = df.filter(F.col(col_name) >= filter_val["min"])
            if "max" in filter_val and filter_val["max"] is not None:
                df = df.filter(F.col(col_name) <= filter_val["max"])
            if "start" in filter_val and filter_val["start"]:
                df = df.filter(F.col(col_name) >= filter_val["start"])
            if "end" in filter_val and filter_val["end"]:
                df = df.filter(F.col(col_name) <= filter_val["end"])
        elif isinstance(filter_val, list):
            df = df.filter(F.col(col_name).isin(filter_val))
        elif isinstance(filter_val, str):
            df = df.filter(F.col(col_name).contains(filter_val))
        elif isinstance(filter_val, bool):
            df = df.filter(F.col(col_name) == filter_val)
    return df


def _summary_metrics(df: DataFrame, primary_metric: str | None) -> dict:
    total_records = df.count()
    if not primary_metric or primary_metric not in df.columns:
        return {"total_records": total_records}

    agg = df.agg(
        F.sum(primary_metric).alias("total"),
        F.avg(primary_metric).alias("average"),
        F.min(primary_metric).alias("minimum"),
        F.max(primary_metric).alias("maximum"),
        F.stddev(primary_metric).alias("std_deviation"),
    ).collect()[0]

    top10 = (
        df.select(primary_metric)
        .orderBy(F.col(primary_metric).desc())
        .limit(10)
        .collect()
    )

    return {
        "total_records": total_records,
        "primary_metric": primary_metric,
        "total": _sf(agg["total"]),
        "average": _sf(agg["average"]),
        "minimum": _sf(agg["minimum"]),
        "maximum": _sf(agg["maximum"]),
        "std_deviation": _sf(agg["std_deviation"]),
        "top_10_values": [_sf(r[primary_metric]) for r in top10],
    }


def _category_analysis(df: DataFrame, primary_metric: str | None, category_col: str | None) -> dict:
    if not category_col or category_col not in df.columns:
        # Auto-detect first low-cardinality string col
        for f in df.schema.fields:
            if str(f.dataType) in ("StringType()", "string"):
                unique = df.select(f.name).distinct().count()
                if 2 <= unique <= MAX_CATEGORY_VALUES:
                    category_col = f.name
                    break
    if not category_col:
        return {"available": False, "reason": "No suitable categorical column found."}

    agg_exprs = [F.count("*").alias("count")]
    if primary_metric and primary_metric in df.columns:
        agg_exprs += [
            F.sum(primary_metric).alias("total"),
            F.avg(primary_metric).alias("average"),
        ]

    cat_df = (
        df.groupBy(category_col)
        .agg(*agg_exprs)
        .orderBy(F.col("count").desc())
        .limit(MAX_CATEGORY_VALUES)
    )
    rows = cat_df.collect()
    total_count = sum(r["count"] for r in rows) or 1

    categories = []
    for i, r in enumerate(rows):
        entry = {
            "category": str(r[category_col]),
            "count": r["count"],
            "percentage": round(r["count"] / total_count * 100, 2),
            "rank": i + 1,
        }
        if primary_metric and "total" in r.asDict():
            entry["total"] = _sf(r["total"])
            entry["average"] = _sf(r["average"])
        categories.append(entry)

    return {
        "column": category_col,
        "total_categories": df.select(category_col).distinct().count(),
        "shown": len(categories),
        "categories": categories,
    }


def _time_analysis(df: DataFrame, primary_metric: str | None, time_col: str | None) -> dict:
    if not time_col or time_col not in df.columns:
        return {"available": False, "reason": "No time column detected."}
    if not primary_metric or primary_metric not in df.columns:
        return {"available": False, "reason": "No primary metric for time analysis."}

    # Monthly
    monthly = (
        df.withColumn("year_month", F.date_format(F.col(time_col), "yyyy-MM"))
        .groupBy("year_month")
        .agg(F.sum(primary_metric).alias("total"), F.count("*").alias("count"))
        .orderBy("year_month")
        .limit(60)
        .collect()
    )

    # Yearly
    yearly = (
        df.withColumn("year", F.year(F.col(time_col)))
        .groupBy("year")
        .agg(F.sum(primary_metric).alias("total"), F.count("*").alias("count"))
        .orderBy("year")
        .collect()
    )

    monthly_list = [{"period": r["year_month"], "total": _sf(r["total"]), "count": r["count"]} for r in monthly]
    yearly_list = [{"year": r["year"], "total": _sf(r["total"]), "count": r["count"]} for r in yearly]

    # Growth rate
    growth = None
    if len(yearly_list) >= 2:
        first = yearly_list[0]["total"] or 0
        last = yearly_list[-1]["total"] or 0
        if first > 0:
            growth = round((last - first) / first * 100, 2)

    # Peak detection
    peak_month = max(monthly_list, key=lambda x: x["total"] or 0) if monthly_list else None

    return {
        "available": True,
        "time_column": time_col,
        "monthly": monthly_list,
        "yearly": yearly_list,
        "overall_growth_rate_pct": growth,
        "peak_period": peak_month,
    }


def _distribution_analysis(df: DataFrame, primary_metric: str | None, profile: dict) -> dict:
    if not primary_metric or primary_metric not in df.columns:
        return {"available": False}

    stats = next((p["numeric_stats"] for p in profile["column_profiles"] if p["column"] == primary_metric and p.get("numeric_stats")), None)
    if not stats:
        return {"available": False}

    mn, mx = stats.get("min", 0) or 0, stats.get("max", 1) or 1
    if mn == mx:
        return {"available": False, "reason": "All values identical."}

    bucket_width = (mx - mn) / HISTOGRAM_BINS
    buckets = []
    for i in range(HISTOGRAM_BINS):
        lo = mn + i * bucket_width
        hi = lo + bucket_width
        count = df.filter((F.col(primary_metric) >= lo) & (F.col(primary_metric) < hi)).count()
        buckets.append({"range_start": round(lo, 4), "range_end": round(hi, 4), "count": count})

    pcts = df.approxQuantile(primary_metric, [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99], 0.01)

    return {
        "available": True,
        "column": primary_metric,
        "histogram_bins": buckets,
        "percentiles": {
            "p10": _sf(pcts[0]) if pcts else None,
            "p25": _sf(pcts[1]) if pcts else None,
            "p50": _sf(pcts[2]) if pcts else None,
            "p75": _sf(pcts[3]) if pcts else None,
            "p90": _sf(pcts[4]) if pcts else None,
            "p95": _sf(pcts[5]) if pcts else None,
            "p99": _sf(pcts[6]) if pcts else None,
        },
        "segments": _range_segments(mn, mx),
    }


def _range_segments(mn, mx) -> list:
    span = mx - mn
    return [
        {"label": "Low", "start": round(mn, 4), "end": round(mn + span * 0.33, 4)},
        {"label": "Medium", "start": round(mn + span * 0.33, 4), "end": round(mn + span * 0.66, 4)},
        {"label": "High", "start": round(mn + span * 0.66, 4), "end": round(mx, 4)},
    ]


def _correlation_analysis(df: DataFrame, profile: dict) -> dict:
    numeric_cols = profile["numeric_columns"][:8]  # cap for performance
    avail = [c for c in numeric_cols if c in df.columns]
    if len(avail) < 2:
        return {"available": False, "reason": "Need at least 2 numeric columns."}

    matrix = []
    for c1 in avail:
        row = {"column": c1, "correlations": {}}
        for c2 in avail:
            try:
                corr = df.stat.corr(c1, c2)
                row["correlations"][c2] = round(corr, 4) if corr is not None else None
            except Exception:
                row["correlations"][c2] = None
        matrix.append(row)

    strong_pairs = []
    for i, c1 in enumerate(avail):
        for c2 in avail[i+1:]:
            c = matrix[i]["correlations"].get(c2)
            if c is not None and abs(c) >= 0.5:
                strong_pairs.append({
                    "col1": c1, "col2": c2,
                    "correlation": c,
                    "strength": "strong" if abs(c) >= 0.7 else "moderate",
                    "direction": "positive" if c > 0 else "negative",
                })

    return {
        "available": True,
        "columns": avail,
        "matrix": matrix,
        "strong_correlations": strong_pairs,
    }


def _sf(val) -> float | None:
    try:
        return round(float(val), 4) if val is not None else None
    except Exception:
        return None
