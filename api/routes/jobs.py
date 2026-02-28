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
from fastapi.responses import JSONResponse

from api import db, storage, runner
from api.models import CreateJobResponse, JobStatus, StepStatus, JobHistory

router = APIRouter()


@router.post("/create", response_model=CreateJobResponse)
async def create_job(
    pid_file: UploadFile = File(..., description="PID spreadsheet (.xlsx or .csv)"),
    bank_files: list[UploadFile] = File(..., description="Bank PDF(s)"),
):
    """
    Accept uploaded files, save to storage, create a DB job record,
    launch the pipeline in a background thread, return the job_id.
    """
    job_id = str(uuid.uuid4())
    db.create_job(job_id)

    udir = storage.uploads_dir(job_id)

    # Save PID file
    pid_path = udir / pid_file.filename
    pid_path.write_bytes(await pid_file.read())

    # Save bank PDFs
    bank_paths: list[Path] = []
    for bf in bank_files:
        bp = udir / bf.filename
        bp.write_bytes(await bf.read())
        bank_paths.append(bp)

    # Seed data directories for the existing pipeline
    # The runner will copy files into config.DATA_PID_RAW and DATA_BANK_RAW
    import shutil
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from config import DATA_PID_RAW, DATA_BANK_RAW

    # Clear and repopulate raw data dirs so the pipeline uses these uploads
    for f in DATA_PID_RAW.glob("*"):
        f.unlink(missing_ok=True)
    shutil.copy2(pid_path, DATA_PID_RAW / pid_file.filename)

    for f in DATA_BANK_RAW.glob("*"):
        f.unlink(missing_ok=True)
    for bp in bank_paths:
        shutil.copy2(bp, DATA_BANK_RAW / bp.name)

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
