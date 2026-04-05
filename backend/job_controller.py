import uuid
import traceback
from datetime import datetime, timezone
from backend.firebase_config import get_db
from backend.dataset_catalog import get_dataset_meta
from backend.dataset_loader import load_dataset, get_cached
from backend.profiling_engine import profile_dataset
from backend.cleaning_pipeline import clean_dataset
from backend.data_quality_engine import compute_quality_score
from backend.analytics_engine import run_analytics
from backend.execution_metrics_engine import ExecutionTracker
from backend.visualization_engine import recommend_visualizations
from backend.filter_engine import generate_filter_spec
from backend.insight_generator import generate_insights
from backend.result_formatter import format_result
from backend.logger import get_logger

logger = get_logger(__name__)

# ── Firestore collection names ────────────────────────────────────────────────
# Jobs and results are stored BOTH:
#   1. Under users/{uid}/jobs/{job_id}         ← for per-user hierarchy
#   2. Under users/{uid}/job_results/{job_id}  ← for per-user hierarchy
# All lookups use the uid that is stored inside the job document.

def _user_ref(uid: str):
    return get_db().collection("users").document(uid)

def _job_ref(uid: str, job_id: str):
    """users/{uid}/jobs/{job_id}"""
    return _user_ref(uid).collection("jobs").document(job_id)

def _result_ref(uid: str, job_id: str):
    """users/{uid}/job_results/{job_id}"""
    return _user_ref(uid).collection("job_results").document(job_id)


def create_job(uid: str, dataset_name: str) -> str:
    job_id = str(uuid.uuid4())
    meta = get_dataset_meta(dataset_name)
    if not meta:
        raise ValueError(f"Dataset '{dataset_name}' not found in catalog.")

    _job_ref(uid, job_id).set({
        "job_id":          job_id,
        "uid":             uid,
        "dataset_name":    dataset_name,
        "dataset_version": meta.get("version", "unknown"),
        "status":          "queued",
        "start_time":      datetime.now(timezone.utc).isoformat(),
        "updated_at":      datetime.now(timezone.utc).isoformat(),
        "error":           None,
        "retry_count":     0,
    })
    logger.info(f"Job {job_id} created for user {uid} on dataset '{dataset_name}'")
    return job_id


def _update_status(uid: str, job_id: str, status: str, extra: dict = None):
    payload = {"status": status, "updated_at": datetime.now(timezone.utc).isoformat()}
    if extra:
        payload.update(extra)
    _job_ref(uid, job_id).update(payload)
    logger.info(f"Job {job_id} → {status}")


def run_job(job_id: str, uid: str, dataset_name: str, filters: dict = None):
    tracker = ExecutionTracker()
    tracker.start()
    meta = get_dataset_meta(dataset_name)

    try:
        # ── Profiling ──────────────────────────────────────────────────────────
        _update_status(uid, job_id, "profiling")
        tracker.begin_stage("load")
        df_raw = load_dataset(meta)
        tracker.end_stage("load")

        tracker.begin_stage("profiling")
        profile = profile_dataset(df_raw)
        tracker.end_stage("profiling")

        # ── Cleaning ───────────────────────────────────────────────────────────
        _update_status(uid, job_id, "cleaning")
        tracker.begin_stage("cleaning")
        df_clean, cleaning_summary = clean_dataset(df_raw, meta, profile)
        tracker.end_stage("cleaning")

        clean_profile = profile_dataset(df_clean)

        # ── Quality Scoring ────────────────────────────────────────────────────
        _update_status(uid, job_id, "quality_scoring")
        tracker.begin_stage("quality_scoring")
        quality_score = compute_quality_score(df_clean, clean_profile, meta)
        tracker.end_stage("quality_scoring")

        # ── Analytics ─────────────────────────────────────────────────────────
        _update_status(uid, job_id, "analytics")
        tracker.begin_stage("analytics")
        analytics = run_analytics(df_clean, meta, clean_profile, filters)
        tracker.end_stage("analytics")

        # ── Filters + Viz + Insights ───────────────────────────────────────────
        tracker.begin_stage("post_processing")
        filter_spec = generate_filter_spec(df_clean, clean_profile)
        viz_recs    = recommend_visualizations(analytics, clean_profile, meta)
        insights    = generate_insights(analytics, quality_score, clean_profile, meta)
        tracker.end_stage("post_processing")

        # ── Execution Metrics ──────────────────────────────────────────────────
        row_count    = clean_profile.get("row_count", 0)
        exec_metrics = tracker.capture(df_clean, row_count)

        # ── Format + Store ─────────────────────────────────────────────────────
        _update_status(uid, job_id, "formatting")
        result = format_result(
            job_id, uid, meta, clean_profile, cleaning_summary,
            quality_score, analytics, exec_metrics, insights, viz_recs, filter_spec
        )

        # Store result under users/{uid}/job_results/{job_id}
        _result_ref(uid, job_id).set(result)

        _update_status(uid, job_id, "completed", {
            "end_time":             datetime.now(timezone.utc).isoformat(),
            "execution_time_sec":   exec_metrics["total_execution_time_sec"],
            "row_count":            row_count,
            "quality_score":        quality_score["overall_score"],
        })
        logger.info(f"Job {job_id} completed in {exec_metrics['total_execution_time_sec']}s")
        return result

    except Exception as e:
        err = traceback.format_exc()
        logger.error(f"Job {job_id} FAILED: {err}")
        _update_status(uid, job_id, "failed", {"error": str(e), "traceback": err[:2000]})
        raise


def get_job_status(job_id: str, uid: str) -> dict:
    """Fetch job from users/{uid}/jobs/{job_id}."""
    doc = _job_ref(uid, job_id).get()
    if not doc.exists:
        raise ValueError(f"Job '{job_id}' not found.")
    return doc.to_dict()


def get_job_result(job_id: str, uid: str) -> dict:
    """Fetch result from users/{uid}/job_results/{job_id}."""
    doc = _result_ref(uid, job_id).get()
    if not doc.exists:
        raise ValueError(f"Result for job '{job_id}' not found.")
    return doc.to_dict()


def get_user_jobs(uid: str) -> list:
    """All jobs for a user from users/{uid}/jobs, newest first."""
    docs = (
        _user_ref(uid).collection("jobs")
        .order_by("start_time", direction="DESCENDING")
        .limit(50)
        .stream()
    )
    return [d.to_dict() for d in docs]


def get_all_jobs(limit: int = 100) -> list:
    """Admin: fetch jobs across ALL users via collection group query."""
    db = get_db()
    docs = (
        db.collection_group("jobs")
        .order_by("start_time", direction="DESCENDING")
        .limit(limit)
        .stream()
    )
    return [d.to_dict() for d in docs]


def retry_job(job_id: str, uid: str) -> str:
    old = get_job_status(job_id, uid)
    dataset_name = old.get("dataset_name")
    new_job_id = create_job(uid, dataset_name)
    logger.info(f"Retry: new job {new_job_id} created from failed job {job_id}")
    return new_job_id


def delete_job(job_id: str, uid: str) -> None:
    """Delete job + result from under users/{uid}."""
    _job_ref(uid, job_id).delete()
    _result_ref(uid, job_id).delete()
    logger.info(f"Job {job_id} deleted for user {uid}")