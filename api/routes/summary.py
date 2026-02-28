from __future__ import annotations
"""
api/routes/summary.py

GET /api/jobs/{job_id}/summary
GET /api/jobs/{job_id}/coverage
"""

from fastapi import APIRouter, HTTPException
from api import storage
from api.models import Summary, Coverage, MatchBreakdown, CoverageBank, CoveragePeriod

router = APIRouter()


def _require_artifact(job_id: str, filename: str) -> object:
    try:
        return storage.read_json(job_id, filename)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Artifact '{filename}' not ready for job {job_id!r}. "
                   "Wait for the job to complete.",
        )


@router.get("/{job_id}/summary", response_model=Summary)
async def get_summary(job_id: str):
    data = _require_artifact(job_id, "summary.json")
    bd = data.get("breakdown", {})
    return Summary(
        job_id=job_id,
        total_pid_records=data.get("total_pid_records", 0),
        total_bank_transactions=data.get("total_bank_transactions", 0),
        matched=data.get("matched", 0),
        unmatched=data.get("unmatched", 0),
        match_rate=data.get("match_rate", 0.0),
        total_pid_amount=data.get("total_pid_amount", 0.0),
        total_matched_amount=data.get("total_matched_amount", 0.0),
        breakdown=MatchBreakdown(
            primary=bd.get("primary", 0),
            secondary=bd.get("secondary", 0),
            retention=bd.get("retention", 0),
            fuzzy=bd.get("fuzzy", 0),
            unmatched=bd.get("unmatched", 0),
        ),
        banks_loaded=data.get("banks_loaded", []),
    )


@router.get("/{job_id}/coverage", response_model=Coverage)
async def get_coverage(job_id: str):
    data = _require_artifact(job_id, "coverage.json")
    banks = []
    for b in data.get("banks", []):
        periods = [
            CoveragePeriod(
                year=p["year"],
                month=p["month"],
                month_label=p["month_label"],
                transaction_count=p["transaction_count"],
            )
            for p in b.get("periods", [])
        ]
        banks.append(CoverageBank(
            bank=b["bank"],
            periods=periods,
            total_transactions=b.get("total_transactions", 0),
        ))
    return Coverage(job_id=job_id, banks=banks)
