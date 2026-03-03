from __future__ import annotations
"""
api/runner.py — Background thread executor for pipeline stages.

Called by POST /api/jobs/create after saving uploaded files.
Runs the full reconciliation pipeline and writes all artifacts to storage/<job_id>/.
"""

import sys
import logging
import threading
import traceback
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from api import db, storage

logger = logging.getLogger(__name__)


# ── Step progress weights (must sum to 100) ────────────────────────────────────
STEP_PROGRESS = {
    "parse_pid":            10,
    "parse_banks":          20,
    "canonicalize_vendors": 10,
    "match":                20,
    "report":               15,
    "build_artifacts":      25,
}


def _step(job_id: str, step_name: str, fn, *args, **kwargs):
    """Run a pipeline step, updating DB status before/after."""
    db.update_step(job_id, step_name, "running")
    storage.append_log(job_id, f"[{_now()}] START {step_name}")
    try:
        result = fn(*args, **kwargs)
        db.update_step(job_id, step_name, "done")
        storage.append_log(job_id, f"[{_now()}] DONE  {step_name}")
        return result
    except Exception as exc:
        db.update_step(job_id, step_name, "error")
        storage.append_log(job_id, f"[{_now()}] ERROR {step_name}: {exc}")
        raise


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def _advance_progress(job_id: str, step_name: str, current: list[int]):
    current[0] += STEP_PROGRESS.get(step_name, 0)
    db.update_job(job_id, progress=min(current[0], 95))


# ── Artifact builders ──────────────────────────────────────────────────────────

def _build_summary(job_id: str, pid_df, bank_df, match_df) -> dict:
    import pandas as pd

    counts = match_df["match_type"].value_counts().to_dict()
    matched = match_df[match_df["match_type"] != "unmatched"]

    # Total matched PID amount
    pid_idx = pid_df.set_index("pid_id")
    matched_amounts = []
    for pid_id in matched["pid_id"]:
        if pid_id in pid_idx.index:
            amt = pid_idx.loc[pid_id].get("amount", 0) or 0
            matched_amounts.append(float(abs(amt)))

    total_pid_amount = float(pid_df["amount"].abs().sum())
    total_matched = sum(matched_amounts)

    banks_loaded = sorted(bank_df["bank_name"].unique().tolist()) if "bank_name" in bank_df.columns else []

    summary = {
        "job_id": job_id,
        "total_pid_records": len(pid_df),
        "total_bank_transactions": len(bank_df),
        "matched": len(matched),
        "unmatched": counts.get("unmatched", 0),
        "match_rate": round(len(matched) / max(len(pid_df), 1) * 100, 1),
        "total_pid_amount": round(total_pid_amount, 2),
        "total_matched_amount": round(total_matched, 2),
        "breakdown": {
            "primary":   counts.get("primary", 0),
            "secondary": counts.get("secondary", 0),
            "retention": counts.get("retention", 0),
            "fuzzy":     counts.get("fuzzy", 0),
            "unmatched": counts.get("unmatched", 0),
        },
        "banks_loaded": banks_loaded,
    }
    storage.write_json(job_id, "summary.json", summary)
    return summary


def _build_coverage(job_id: str, bank_df) -> dict:
    import pandas as pd

    coverage_banks: list[dict] = []
    bank_groups = bank_df.groupby("bank_name") if "bank_name" in bank_df.columns else []

    for bank_name, grp in bank_groups:
        periods: list[dict] = []
        if "posted_date" in grp.columns:
            grp = grp.copy()
            grp["_date"] = pd.to_datetime(grp["posted_date"], errors="coerce")
            monthly = grp.dropna(subset=["_date"]).groupby(
                grp.dropna(subset=["_date"])["_date"].dt.to_period("M")
            ).size()
            for period, cnt in monthly.items():
                periods.append({
                    "year": period.year,
                    "month": period.month,
                    "month_label": period.strftime("%b %Y"),
                    "transaction_count": int(cnt),
                })
        coverage_banks.append({
            "bank": str(bank_name),
            "periods": periods,
            "total_transactions": int(len(grp)),
        })

    coverage = {"job_id": job_id, "banks": coverage_banks}
    storage.write_json(job_id, "coverage.json", coverage)
    return coverage


def _build_review_queue(job_id: str, pid_df, bank_df, match_df) -> dict:
    """Build review queue: fuzzy + secondary + retention matches need review."""
    pid_idx = pid_df.set_index("pid_id")
    bank_idx = bank_df.set_index("bank_id") if "bank_id" in bank_df.columns else {}

    NEEDS_REVIEW = {"fuzzy", "secondary", "retention"}
    items: list[dict] = []

    for _, mrow in match_df.iterrows():
        if mrow["match_type"] not in NEEDS_REVIEW:
            continue

        pid_id = mrow["pid_id"]
        if pid_id not in pid_idx.index:
            continue

        prow = pid_idx.loc[pid_id]
        bank_id = mrow.get("bank_id")

        bank_amount = None
        bank_posted_date = None
        bank_description = None

        if bank_id and "+" not in str(bank_id):
            if bank_id in bank_idx.index:
                brow = bank_idx.loc[bank_id]
                bank_amount = float(abs(brow.get("amount", 0) or 0))
                bank_posted_date = str(brow.get("posted_date", ""))
                bank_description = str(brow.get("description", ""))

        match_id = f"{pid_id}:{bank_id or 'none'}"
        items.append({
            "match_id":          match_id,
            "pid_id":            pid_id,
            "bank_id":           bank_id,
            "vendor":            str(prow.get("vendor", "")),
            "invoice_no":        str(prow.get("invoice_no", "")),
            "pid_amount":        float(abs(prow.get("amount", 0) or 0)),
            "bank_amount":       bank_amount,
            "match_type":        mrow["match_type"],
            "match_confidence":  float(mrow["match_confidence"]),
            "notes":             str(mrow.get("notes", "")),
            "status":            "needs_review",
            "check_no":          str(prow.get("check_no", "")),
            "check_date":        str(prow.get("check_date", "")),
            "bank_posted_date":  bank_posted_date,
            "bank_description":  bank_description,
            "bank":              str(prow.get("bank", "")),
        })

    queue = {
        "job_id":   job_id,
        "items":    items,
        "total":    len(items),
        "pending":  len(items),
        "approved": 0,
        "rejected": 0,
    }
    storage.write_json(job_id, "review_queue.json", queue)
    return queue


def _build_raw_ocr(job_id: str, bank_df, parse_metadata: dict | None = None) -> dict:
    parse_metadata = parse_metadata or {}
    files = parse_metadata.get("files", [])

    llm_rows = []
    if "source" in bank_df.columns:
        llm_rows_df = bank_df[bank_df["source"] == "llm_fallback"].copy()
        for _, row in llm_rows_df.iterrows():
            llm_rows.append({
                "bank_id": str(row.get("bank_id", "")),
                "posted_date": str(row.get("posted_date", "")),
                "check_no": str(row.get("check_no", "")),
                "amount": float(row.get("amount", 0) or 0),
                "description": str(row.get("description", "")),
                "statement_month": str(row.get("statement_month", "")),
                "bank_name": str(row.get("bank_name", "")),
            })

    total_llm_pages = 0
    total_llm_new_records = 0
    total_llm_rows_extracted = 0
    total_vendor_enriched = 0
    for fmeta in files:
        llm = fmeta.get("llm_stats", {}) or {}
        total_llm_pages += int(llm.get("pages_processed", 0) or 0)
        total_llm_new_records += int(llm.get("new_records", 0) or 0)
        total_llm_rows_extracted += int(llm.get("rows_extracted", 0) or 0)
        total_vendor_enriched += int(llm.get("vendor_matches", 0) or 0)

    artifact = {
        "job_id": job_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": files,
        "summary": {
            "total_files": int(parse_metadata.get("total_files", len(files))),
            "llm_pages_processed": total_llm_pages,
            "llm_rows_extracted": total_llm_rows_extracted,
            "llm_new_records": total_llm_new_records,
            "llm_vendor_enriched": total_vendor_enriched,
            "llm_records_in_final_bank_df": len(llm_rows),
        },
        "llm_fallback_records": llm_rows,
    }
    storage.write_json(job_id, "raw_ocr.json", artifact)
    return artifact


# ── Main runner ────────────────────────────────────────────────────────────────

def run_job(job_id: str, pid_path: Path, bank_paths: list[Path]):
    """Entry point called from background thread."""
    progress = [0]
    jdir = storage.job_dir(job_id)

    try:
        db.update_job(job_id, state="running", progress=0)
        storage.append_log(job_id, f"[{_now()}] Job {job_id} started")

        # ── Import pipeline modules ─────────────────────────────────────────
        from src.ingest.parse_pid      import run as parse_pid_run
        from src.ingest.parse_bank_pdf import run as parse_banks_run
        from match.harmonize_records   import load_and_harmonize
        from match.fuzzy_llm           import add_canonical_vendor_column
        from match.deterministic       import run_matching
        from output.reconciliation_report import generate_report
        job_interim_dir = jdir
        bank_parse_metadata: dict = {"files": [], "total_files": 0}

        # ── Stage 1: Parse PID ──────────────────────────────────────────────
        def _parse_pid():
            df = parse_pid_run(pid_path, output_path=job_interim_dir / "pid.csv")
            storage.append_log(job_id, f"  → {len(df)} PID records")
            return df

        pid_df = _step(job_id, "parse_pid", _parse_pid)
        _advance_progress(job_id, "parse_pid", progress)

        # ── Stage 2: Parse Bank PDFs ────────────────────────────────────────
        def _parse_banks():
            nonlocal bank_parse_metadata
            bank_dir = bank_paths[0].parent if bank_paths else None
            results, metadata = parse_banks_run(
                bank_dir,
                interim_dir=job_interim_dir,
                return_metadata=True,
            )
            bank_parse_metadata = metadata
            total = 0
            for bank, df in results.items():
                total += len(df)
            storage.append_log(job_id, f"  → {total} bank transactions across {len(results)} banks")
            return results

        _step(job_id, "parse_banks", _parse_banks)
        _advance_progress(job_id, "parse_banks", progress)

        # ── Stage 3: Load and harmonize from interim ────────────────────────
        pid_df, bank_df = load_and_harmonize(job_interim_dir)

        # ── Stage 3b: Canonicalize vendors ─────────────────────────────────
        def _canonicalize():
            return add_canonical_vendor_column(bank_df, pid_df)

        bank_df = _step(job_id, "canonicalize_vendors", _canonicalize)
        _advance_progress(job_id, "canonicalize_vendors", progress)

        # ── Stage 4: Match ──────────────────────────────────────────────────
        def _match():
            return run_matching(pid_df, bank_df)

        match_df = _step(job_id, "match", _match)
        _advance_progress(job_id, "match", progress)

        # ── Stage 5: Generate Excel reports ────────────────────────────────
        def _report():
            return generate_report(pid_df, bank_df, match_df, output_dir=jdir)

        reconciled_path, unmatched_path = _step(job_id, "report", _report)
        _advance_progress(job_id, "report", progress)

        # ── Stage 6: Build JSON artifacts ──────────────────────────────────
        def _build_artifacts():
            _build_summary(job_id, pid_df, bank_df, match_df)
            _build_coverage(job_id, bank_df)
            _build_review_queue(job_id, pid_df, bank_df, match_df)
            _build_raw_ocr(job_id, bank_df, bank_parse_metadata)

        _step(job_id, "build_artifacts", _build_artifacts)
        _advance_progress(job_id, "build_artifacts", progress)

        db.update_job(job_id, state="completed", progress=100)
        storage.append_log(job_id, f"[{_now()}] Job {job_id} COMPLETE")

    except Exception as exc:
        tb = traceback.format_exc()
        logger.error(f"Job {job_id} failed: {exc}\n{tb}")
        storage.append_log(job_id, f"[{_now()}] FATAL: {exc}")
        db.update_job(job_id, state="error", error_message=str(exc))


def launch(job_id: str, pid_path: Path, bank_paths: list[Path]):
    """Launch pipeline in a daemon background thread."""
    t = threading.Thread(
        target=run_job,
        args=(job_id, pid_path, bank_paths),
        daemon=True,
        name=f"job-{job_id}",
    )
    t.start()
    return t
