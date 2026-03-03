from __future__ import annotations
"""
api/models.py — Pydantic request/response models for all API routes.
"""

from typing import List, Optional, Literal
from pydantic import BaseModel, Field

StepName = Literal[
    "parse_pid",
    "parse_banks",
    "canonicalize_vendors",
    "match",
    "report",
    "build_artifacts",
]
StepState = Literal["pending", "running", "done", "error"]
JobState = Literal["pending", "running", "completed", "error"]
ReviewStatus = Literal["needs_review", "approved", "rejected"]
MatchType = Literal["primary", "secondary", "retention", "fuzzy", "unmatched"]


# ── Job Models ────────────────────────────────────────────────────────────────

class StepStatus(BaseModel):
    step_name: StepName
    status: StepState


class JobStatus(BaseModel):
    job_id: str
    state: JobState
    progress: int  # 0–100
    error_message: Optional[str] = None
    steps: List[StepStatus] = Field(default_factory=list)
    created_at: str
    updated_at: str


class JobHistory(BaseModel):
    job_id: str
    state: JobState
    created_at: str
    updated_at: str
    error_message: Optional[str] = None


class CreateJobResponse(BaseModel):
    job_id: str


# ── Summary Models ────────────────────────────────────────────────────────────

class MatchBreakdown(BaseModel):
    primary: int = 0
    secondary: int = 0
    retention: int = 0
    fuzzy: int = 0
    unmatched: int = 0


class Summary(BaseModel):
    job_id: str
    total_pid_records: int
    total_bank_transactions: int
    matched: int
    unmatched: int
    match_rate: float
    total_pid_amount: float
    total_matched_amount: float
    breakdown: MatchBreakdown
    banks_loaded: List[str]


class CoveragePeriod(BaseModel):
    year: int
    month: int
    month_label: str
    transaction_count: int


class CoverageBank(BaseModel):
    bank: str
    periods: List[CoveragePeriod]
    total_transactions: int


class Coverage(BaseModel):
    job_id: str
    banks: List[CoverageBank] = Field(default_factory=list)


# ── Review Models ──────────────────────────────────────────────────────────────

class ReviewItem(BaseModel):
    match_id: str
    pid_id: str
    bank_id: Optional[str] = None
    vendor: str
    invoice_no: str
    pid_amount: float
    bank_amount: Optional[float] = None
    match_type: MatchType
    match_confidence: float
    notes: str
    status: ReviewStatus
    check_no: str
    check_date: Optional[str] = None
    bank_posted_date: Optional[str] = None
    bank_description: Optional[str] = None
    bank: str


class ReviewQueue(BaseModel):
    job_id: str
    items: List[ReviewItem] = Field(default_factory=list)
    total: int
    pending: int
    approved: int
    rejected: int


class MatchDetail(BaseModel):
    match_id: str
    pid_id: str
    bank_id: Optional[str] = None
    vendor: str
    invoice_no: str
    invoice_date: Optional[str] = None
    pid_amount: float
    bank_amount: Optional[float] = None
    amount_diff: Optional[float] = None
    match_type: MatchType
    match_confidence: float
    notes: str
    status: ReviewStatus
    check_no: str
    check_date: Optional[str] = None
    bank_posted_date: Optional[str] = None
    bank_description: Optional[str] = None
    bank: str
    phase: Optional[str] = None
    reference: Optional[str] = None


class ApproveRejectResponse(BaseModel):
    match_id: str
    status: ReviewStatus
    message: str


# ── Packet Models ──────────────────────────────────────────────────────────────

class PacketGenerateResponse(BaseModel):
    match_id: str
    packet_url: str


class PacketInfo(BaseModel):
    match_id: str
    filename: str
    url: str
    generated_at: str


# ── Export Models ──────────────────────────────────────────────────────────────

class ExportLinks(BaseModel):
    reconciled_xlsx: str
    unmatched_xlsx: str
    summary_json: str
    raw_ocr_json: Optional[str] = None


class PDFSource(BaseModel):
    filename: str
    page_count: int


class PDFSourcesResponse(BaseModel):
    job_id: str
    sources: List[PDFSource] = Field(default_factory=list)
