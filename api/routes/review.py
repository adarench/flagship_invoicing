from __future__ import annotations
"""
api/routes/review.py

GET  /api/jobs/{job_id}/review_queue
GET  /api/jobs/{job_id}/match/{match_id}
POST /api/jobs/{job_id}/match/{match_id}/approve
POST /api/jobs/{job_id}/match/{match_id}/reject
"""

from fastapi import APIRouter, HTTPException
import pandas as pd
from api import storage
from api.models import ReviewQueue, ReviewItem, MatchDetail, ApproveRejectResponse

router = APIRouter()


def _load_queue(job_id: str) -> dict:
    try:
        return storage.read_json(job_id, "review_queue.json")
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Review queue not ready for job {job_id!r}.",
        )


def _load_pid_bank_frames(job_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    pid_path = storage.artifact_path(job_id, "pid.csv")
    if not pid_path.exists():
        raise HTTPException(status_code=404, detail=f"PID artifact not ready for job {job_id!r}")
    pid_df = pd.read_csv(pid_path).fillna("")

    bank_files = sorted(storage.job_dir(job_id).glob("bank_*.csv"))
    if not bank_files:
        bank_df = pd.DataFrame(columns=["bank_id"])
    else:
        bank_df = pd.concat([pd.read_csv(p).fillna("") for p in bank_files], ignore_index=True)
    return pid_df, bank_df


@router.get("/{job_id}/review_queue", response_model=ReviewQueue)
async def get_review_queue(job_id: str):
    data = _load_queue(job_id)
    items = [ReviewItem(**i) for i in data.get("items", [])]
    approved = sum(1 for i in items if i.status == "approved")
    rejected = sum(1 for i in items if i.status == "rejected")
    pending  = sum(1 for i in items if i.status == "needs_review")
    return ReviewQueue(
        job_id=job_id,
        items=items,
        total=len(items),
        pending=pending,
        approved=approved,
        rejected=rejected,
    )


@router.get("/{job_id}/match/{match_id}", response_model=MatchDetail)
async def get_match_detail(job_id: str, match_id: str):
    data = _load_queue(job_id)
    pid_df, bank_df = _load_pid_bank_frames(job_id)
    pid_idx = pid_df.set_index("pid_id") if not pid_df.empty else pd.DataFrame()
    bank_idx = bank_df.set_index("bank_id") if not bank_df.empty and "bank_id" in bank_df.columns else pd.DataFrame()
    for item in data.get("items", []):
        if item["match_id"] == match_id:
            pid_row = pid_idx.loc[item["pid_id"]] if isinstance(pid_idx, pd.DataFrame) and item["pid_id"] in pid_idx.index else {}

            bank_amount = item.get("bank_amount")
            bank_posted_date = item.get("bank_posted_date")
            bank_description = item.get("bank_description")
            bank_id = item.get("bank_id")
            if bank_id and "+" not in str(bank_id) and isinstance(bank_idx, pd.DataFrame) and bank_id in bank_idx.index:
                brow = bank_idx.loc[bank_id]
                bank_amount = float(abs(brow.get("amount", 0) or 0))
                bank_posted_date = str(brow.get("posted_date", "") or "")
                bank_description = str(brow.get("description", "") or "")

            # Enrich with additional fields for detail view
            return MatchDetail(
                match_id=item["match_id"],
                pid_id=item["pid_id"],
                bank_id=bank_id,
                vendor=str(pid_row.get("vendor", item.get("vendor", ""))),
                invoice_no=str(pid_row.get("invoice_no", item.get("invoice_no", ""))),
                invoice_date=str(pid_row.get("invoice_date", "")) or None,
                pid_amount=float(abs(pid_row.get("amount", item.get("pid_amount", 0.0)) or 0)),
                bank_amount=bank_amount,
                amount_diff=round(
                    abs((item.get("pid_amount") or 0) - (bank_amount or 0)), 2
                ),
                match_type=item.get("match_type", ""),
                match_confidence=item.get("match_confidence", 0.0),
                notes=item.get("notes", ""),
                status=item.get("status", "needs_review"),
                check_no=str(pid_row.get("check_no", item.get("check_no", ""))),
                check_date=str(pid_row.get("check_date", item.get("check_date", ""))) or None,
                bank_posted_date=bank_posted_date,
                bank_description=bank_description,
                bank=str(pid_row.get("bank", item.get("bank", ""))),
                phase=str(pid_row.get("phase", "")) or None,
                reference=str(pid_row.get("reference", "")) or None,
            )
    raise HTTPException(status_code=404, detail=f"Match {match_id!r} not found in job {job_id!r}")


def _update_match_status(job_id: str, match_id: str, new_status: str) -> dict:
    data = _load_queue(job_id)
    found = False
    for item in data.get("items", []):
        if item["match_id"] == match_id:
            item["status"] = new_status
            found = True
            break
    if not found:
        raise HTTPException(status_code=404, detail=f"Match {match_id!r} not found")

    # Recompute counts
    items = data["items"]
    data["total"]    = len(items)
    data["approved"] = sum(1 for i in items if i["status"] == "approved")
    data["rejected"] = sum(1 for i in items if i["status"] == "rejected")
    data["pending"]  = sum(1 for i in items if i["status"] == "needs_review")

    storage.write_json(job_id, "review_queue.json", data)
    return data


@router.post("/{job_id}/match/{match_id}/approve", response_model=ApproveRejectResponse)
async def approve_match(job_id: str, match_id: str):
    _update_match_status(job_id, match_id, "approved")
    return ApproveRejectResponse(
        match_id=match_id,
        status="approved",
        message="Match approved successfully",
    )


@router.post("/{job_id}/match/{match_id}/reject", response_model=ApproveRejectResponse)
async def reject_match(job_id: str, match_id: str):
    _update_match_status(job_id, match_id, "rejected")
    return ApproveRejectResponse(
        match_id=match_id,
        status="rejected",
        message="Match rejected",
    )
