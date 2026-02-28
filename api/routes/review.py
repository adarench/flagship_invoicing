from __future__ import annotations
"""
api/routes/review.py

GET  /api/jobs/{job_id}/review_queue
GET  /api/jobs/{job_id}/match/{match_id}
POST /api/jobs/{job_id}/match/{match_id}/approve
POST /api/jobs/{job_id}/match/{match_id}/reject
"""

from fastapi import APIRouter, HTTPException
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
    for item in data.get("items", []):
        if item["match_id"] == match_id:
            # Enrich with additional fields for detail view
            return MatchDetail(
                match_id=item["match_id"],
                pid_id=item["pid_id"],
                bank_id=item.get("bank_id"),
                vendor=item.get("vendor", ""),
                invoice_no=item.get("invoice_no", ""),
                invoice_date=None,
                pid_amount=item.get("pid_amount", 0.0),
                bank_amount=item.get("bank_amount"),
                amount_diff=round(
                    abs((item.get("pid_amount") or 0) - (item.get("bank_amount") or 0)), 2
                ),
                match_type=item.get("match_type", ""),
                match_confidence=item.get("match_confidence", 0.0),
                notes=item.get("notes", ""),
                status=item.get("status", "needs_review"),
                check_no=item.get("check_no", ""),
                check_date=item.get("check_date"),
                bank_posted_date=item.get("bank_posted_date"),
                bank_description=item.get("bank_description"),
                bank=item.get("bank", ""),
                phase=None,
                reference=None,
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
