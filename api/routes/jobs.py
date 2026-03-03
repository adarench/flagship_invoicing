from __future__ import annotations
"""
api/routes/jobs.py — Job lifecycle endpoints.

POST /api/jobs/create
GET  /api/jobs/{job_id}/status
GET  /api/jobs/history
"""

import uuid
from pathlib import Path

from fastapi import APIRouter, File, UploadFile, HTTPException

from api import db, storage, runner
from api.models import CreateJobResponse, JobStatus, StepStatus, JobHistory

router = APIRouter()

_PID_EXTS = {".xlsx", ".xls", ".csv"}
_BANK_EXTS = {".pdf"}


def _safe_name(name: str) -> str:
    # Prevent path traversal and normalize potentially weird upload names.
    return Path(name or "").name.strip()


def _validate_upload_name(name: str, allowed_exts: set[str], role: str) -> str:
    safe = _safe_name(name)
    if not safe:
        raise HTTPException(status_code=400, detail=f"{role} filename is missing")
    ext = Path(safe).suffix.lower()
    if ext not in allowed_exts:
        allowed = ", ".join(sorted(allowed_exts))
        raise HTTPException(
            status_code=400,
            detail=f"{role} must be one of: {allowed}. Got: {safe}",
        )
    return safe


@router.post("/create", response_model=CreateJobResponse)
async def create_job(
    pid_file: UploadFile = File(..., description="PID spreadsheet (.xlsx or .csv)"),
    bank_files: list[UploadFile] = File(..., description="Bank PDF(s)"),
):
    """
    Accept uploaded files, save to storage, create a DB job record,
    launch the pipeline in a background thread, return the job_id.
    """
    if not bank_files:
        raise HTTPException(status_code=400, detail="At least one bank PDF is required")

    job_id = str(uuid.uuid4())
    db.create_job(job_id)

    udir = storage.uploads_dir(job_id)

    # Save PID file
    pid_name = _validate_upload_name(pid_file.filename or "", _PID_EXTS, "pid_file")
    pid_path = udir / pid_name
    pid_path.write_bytes(await pid_file.read())

    # Save bank PDFs
    bank_paths: list[Path] = []
    seen_names: set[str] = set()
    for bf in bank_files:
        bank_name = _validate_upload_name(bf.filename or "", _BANK_EXTS, "bank_file")
        if bank_name in seen_names:
            raise HTTPException(status_code=400, detail=f"Duplicate bank filename: {bank_name}")
        seen_names.add(bank_name)
        bp = udir / bank_name
        bp.write_bytes(await bf.read())
        bank_paths.append(bp)

    storage.write_json(
        job_id,
        "upload_manifest.json",
        {
            "job_id": job_id,
            "pid_file": pid_name,
            "bank_files": [p.name for p in bank_paths],
            "total_bank_files": len(bank_paths),
        },
    )

    runner.launch(job_id, pid_path, bank_paths)

    return CreateJobResponse(job_id=job_id)


@router.get("/history", response_model=list[JobHistory])
async def get_history():
    jobs = db.list_jobs()
    return [
        JobHistory(
            job_id=j["job_id"],
            state=j["state"],
            created_at=j["created_at"],
            updated_at=j["updated_at"],
            error_message=j.get("error_message"),
        )
        for j in jobs
    ]


@router.get("/{job_id}/status", response_model=JobStatus)
async def get_status(job_id: str):
    job = db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")
    return JobStatus(
        job_id=job["job_id"],
        state=job["state"],
        progress=job["progress"],
        error_message=job.get("error_message"),
        steps=[StepStatus(step_name=s["step_name"], status=s["status"]) for s in job.get("steps", [])],
        created_at=job["created_at"],
        updated_at=job["updated_at"],
    )
