import os
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, JSONResponse
from pydantic import BaseModel
from typing import Optional

from backend.firebase_config import initialize_firebase
from backend.auth_middleware import get_current_user, require_role
from backend.dataset_catalog import list_datasets, get_dataset_meta
from backend.job_controller import (
    create_job, run_job, get_job_status, get_job_result,
    get_user_jobs, get_all_jobs, retry_job, delete_job,
)
from backend.report_generator import generate_pdf_report, generate_excel_report
from backend.logger import get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("SparkInsight starting up...")
    initialize_firebase()
    yield
    logger.info("SparkInsight shutting down...")


app = FastAPI(
    title="SparkInsight Analytics Platform",
    description="Enterprise-grade big data analytics platform powered by Apache Spark and Firebase.",
    version="1.0.0",
    lifespan=lifespan,
)

# ── CORS ─────────────────────────────────────────────────────────────────────
# NOTE: allow_origins=["*"] + allow_credentials=True is an INVALID combination.
# Browsers reject it and silently drop the Access-Control-Allow-Origin header,
# causing "Failed to fetch" errors. Always list origins explicitly.
_ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:5500",      # VS Code Live Server
    "http://127.0.0.1:5500",
    "http://localhost:5501",
    "http://127.0.0.1:5501",
    "http://localhost:8080",      # common dev port
    "http://127.0.0.1:8080",
]

# Allow extra origins via environment variable (for Docker / cloud deployments):
# e.g. EXTRA_ORIGINS="https://myapp.com,https://staging.myapp.com"
_extra = os.getenv("EXTRA_ORIGINS", "")
if _extra:
    _ALLOWED_ORIGINS += [o.strip() for o in _extra.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],   # needed for PDF/Excel download filename
)


# ══════════════════════════════════════════════════════════════════════════════
# SCHEMAS
# ══════════════════════════════════════════════════════════════════════════════

class AnalyzeRequest(BaseModel):
    filters: Optional[dict] = None


class PDFReportRequest(BaseModel):
    chart_images: list[dict] = []


# ══════════════════════════════════════════════════════════════════════════════
# DATASET ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/datasets")
async def list_all_datasets(user: dict = Depends(get_current_user)):
    """List all available datasets from the catalog."""
    datasets = list_datasets()
    return {"datasets": datasets, "total": len(datasets)}


@app.get("/dataset/{name}")
async def get_dataset(name: str, user: dict = Depends(get_current_user)):
    """Get metadata for a specific dataset."""
    meta = get_dataset_meta(name)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Dataset '{name}' not found.")
    return meta


@app.get("/dataset/{name}/preview")
async def dataset_preview(
    name: str,
    rows: int = Query(20, ge=1, le=100),
    user: dict = Depends(get_current_user),
):
    """Return a small preview of dataset rows and column names."""
    from backend.dataset_loader import load_dataset

    meta = get_dataset_meta(name)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Dataset '{name}' not found.")

    try:
        df = load_dataset(meta)
        columns = df.columns
        raw_rows = df.limit(rows).collect()
        sample = [
            {col: (str(row[col]) if row[col] is not None else "") for col in columns}
            for row in raw_rows
        ]
        return {
            "dataset_name": name,
            "columns": columns,
            "rows": sample,
            "total_columns": len(columns),
            "preview_rows": len(sample),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Preview failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# ANALYTICS ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/analyze/{dataset_name}")
async def start_analysis(
    dataset_name: str,
    body: AnalyzeRequest,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user),
):
    """Start an analytics job. Job is stored under users/{uid}/jobs/{job_id}."""
    meta = get_dataset_meta(dataset_name)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found.")

    uid    = user["uid"]
    job_id = create_job(uid, dataset_name)

    def _run():
        import traceback
        try:
            logger.info(f"Background job {job_id} starting for dataset '{dataset_name}'")
            run_job(job_id, uid, dataset_name, body.filters)
            logger.info(f"Background job {job_id} finished successfully")
        except Exception as e:
            logger.error(f"Background job {job_id} FAILED: {e}")
            logger.error(f"Full traceback:\n{traceback.format_exc()}")

    background_tasks.add_task(_run)

    return {
        "job_id": job_id,
        "dataset_name": dataset_name,
        "status": "queued",
        "message": "Analysis job started. Poll /job/{job_id}/status for updates.",
    }


# ══════════════════════════════════════════════════════════════════════════════
# JOB ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/job/{job_id}/status")
async def job_status(job_id: str, user: dict = Depends(get_current_user)):
    """
    Poll job status.
    - Regular users: reads from users/{uid}/jobs/{job_id}
    - Admins: can pass ?uid= to read any user's job
    """
    uid = user["uid"]

    # Admins can optionally query another user's job via ?uid=
    # (useful for support/debugging — not exposed to regular users)
    if user["role"] == "admin":
        # Try own jobs first; fall back gracefully
        try:
            return get_job_status(job_id, uid)
        except ValueError:
            pass
        # Admin fallback: search across all users via collection group
        from backend.firebase_config import get_db
        docs = list(
            get_db().collection_group("jobs")
            .where("job_id", "==", job_id)
            .limit(1)
            .stream()
        )
        if not docs:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
        return docs[0].to_dict()

    try:
        return get_job_status(job_id, uid)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/job/{job_id}/result")
async def job_result(job_id: str, user: dict = Depends(get_current_user)):
    """Get full analytics result for a completed job."""
    uid = user["uid"]

    try:
        status = get_job_status(job_id, uid)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if status.get("status") != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not completed. Current status: {status.get('status')}",
        )

    try:
        return get_job_result(job_id, uid)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/job/{job_id}/download")
async def download_report(
    job_id: str,
    format: str = Query("pdf", regex="^(pdf|excel)$"),
    user: dict = Depends(get_current_user),
):
    """Download analytics report in PDF or Excel format."""
    uid = user["uid"]

    try:
        status = get_job_status(job_id, uid)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if status.get("status") != "completed":
        raise HTTPException(status_code=400, detail="Job must be completed before downloading.")

    result = get_job_result(job_id, uid)

    if format == "pdf":
        pdf_bytes = generate_pdf_report(result, [])
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="sparkinsight_{job_id}.pdf"'},
        )
    else:
        excel_bytes = generate_excel_report(result)
        return Response(
            content=excel_bytes,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="sparkinsight_{job_id}.xlsx"'},
        )


@app.post("/job/{job_id}/download/pdf-with-charts")
async def download_pdf_with_charts(
    job_id: str,
    body: PDFReportRequest,
    user: dict = Depends(get_current_user),
):
    """Download PDF report with frontend-rendered Chart.js images embedded."""
    uid = user["uid"]

    try:
        status = get_job_status(job_id, uid)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if status.get("status") != "completed":
        raise HTTPException(status_code=400, detail="Job must be completed.")

    result  = get_job_result(job_id, uid)
    pdf_bytes = generate_pdf_report(result, body.chart_images)
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="sparkinsight_{job_id}.pdf"'},
    )


@app.post("/job/{job_id}/retry")
async def retry_failed_job(
    job_id: str,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user),
):
    """Retry a failed job — generates a new job_id under the same user."""
    uid = user["uid"]

    try:
        old_status = get_job_status(job_id, uid)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if old_status.get("status") != "failed":
        raise HTTPException(status_code=400, detail="Only failed jobs can be retried.")

    new_job_id   = retry_job(job_id, uid)
    dataset_name = old_status["dataset_name"]

    def _run():
        try:
            run_job(new_job_id, uid, dataset_name)
        except Exception as e:
            logger.error(f"Retry job {new_job_id} failed: {e}")

    background_tasks.add_task(_run)
    return {"new_job_id": new_job_id, "original_job_id": job_id, "status": "queued"}


@app.delete("/job/{job_id}")
async def delete_job_endpoint(job_id: str, user: dict = Depends(get_current_user)):
    """Delete a job and its results from under users/{uid}."""
    uid = user["uid"]

    try:
        get_job_status(job_id, uid)   # verify it exists and belongs to this user
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    try:
        delete_job(job_id, uid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete job: {e}")

    return {"deleted": True, "job_id": job_id}


# ══════════════════════════════════════════════════════════════════════════════
# JOB HISTORY
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/jobs/history")
async def job_history(user: dict = Depends(get_current_user)):
    """
    Job history endpoint.
    - Regular users: returns only their own jobs from users/{uid}/jobs
    - Admins: returns jobs from ALL users via collection group query
    """
    if user["role"] == "admin":
        jobs = get_all_jobs()
    else:
        jobs = get_user_jobs(user["uid"])
    return {"jobs": jobs, "total": len(jobs)}


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/admin/jobs")
async def admin_all_jobs(
    limit: int = Query(100, ge=1, le=500),
    user: dict = Depends(require_role("admin")),
):
    """Admin only: fetch jobs across all users."""
    jobs = get_all_jobs(limit)
    return {"jobs": jobs, "total": len(jobs)}


# ══════════════════════════════════════════════════════════════════════════════
# HEALTH
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/health")
async def health():
    return {"status": "ok", "service": "SparkInsight Analytics Platform", "version": "1.0.0"}