from __future__ import annotations
#!/usr/bin/env python3
"""
run_pipeline.py — Invoice Reconciliation Pipeline entry point.

Usage:
    python run_pipeline.py                            # run all stages
    python run_pipeline.py --stage parse_pid
    python run_pipeline.py --stage parse_banks
    python run_pipeline.py --stage match
    python run_pipeline.py --stage report

    python run_pipeline.py --pid data/pid_raw/PID.xlsx --banks data/bank_raw/

Stages (MVP 1 — active):
    1. parse_pid         → data/interim/pid.csv
    2. parse_banks       → data/interim/bank_<BANK>.csv
    3. harmonize         → unified DataFrames (in-memory)
    4. match             → deterministic match results
    5. report            → output/reconciled.xlsx, output/unmatched.xlsx

Stages (MVP 2 — active):
    6. fuzzy_match       → LLM vendor canonicalization (Claude Haiku)
    7. llm_parse_fallback → LLM PDF extraction for banks pdfplumber can't parse

Stages (MVP 3 — active):
    8. packet_reasoning  → LLM audit explanation per matched record (Claude Sonnet)
"""

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from config import (
    DATA_PID_RAW,
    DATA_BANK_RAW,
    OUTPUT_DIR,
    LOG_DIR,
    BANK_PARSE_WORKERS,
    LLM_PAGE_WORKERS,
    LLM_PAGE_MAX_RETRIES,
    LLM_PAGE_RETRY_BASE_SECONDS,
)
from src.ingest.parse_pid         import run as parse_pid_run
from src.ingest.parse_bank_pdf    import run as parse_banks_run
from match.harmonize_records      import load_and_harmonize
from match.deterministic          import run_matching
from match.fuzzy_llm              import add_canonical_vendor_column
from output.packet_reasoning_llm  import batch_generate_reasoning
from output.reconciliation_report import generate_report


# ─── Logging Setup ────────────────────────────────────────────────────────────

def setup_logging(level: str = "INFO") -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "pipeline.log"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file),
        ],
    )
    return logging.getLogger(__name__)


# ─── Directory Bootstrap ──────────────────────────────────────────────────────

def ensure_dirs():
    from config import DATA_PID_RAW, DATA_BANK_RAW, DATA_INTERIM, OUTPUT_DIR, PACKETS_DIR, LOG_DIR
    for d in [DATA_PID_RAW, DATA_BANK_RAW, DATA_INTERIM, OUTPUT_DIR, PACKETS_DIR, LOG_DIR]:
        d.mkdir(parents=True, exist_ok=True)


# ─── Stage Runners ────────────────────────────────────────────────────────────

def stage_parse_pid(args, logger):
    logger.info("=" * 60)
    logger.info("STAGE 1 — Parse PID")
    pid_path = Path(args.pid) if args.pid else None
    df = parse_pid_run(pid_path)
    logger.info(f"PID parse complete: {len(df)} records")
    return df


def stage_parse_banks(args, logger):
    logger.info("=" * 60)
    logger.info("STAGE 2 — Parse Bank PDFs")
    bank_dir = Path(args.banks) if args.banks else DATA_BANK_RAW
    results = parse_banks_run(
        bank_dir,
        workers=args.bank_parse_workers,
        llm_page_workers=args.llm_page_workers,
        llm_page_max_retries=args.llm_page_max_retries,
        llm_page_retry_base_seconds=args.llm_page_retry_base_seconds,
    )
    total = sum(len(df) for df in results.values())
    logger.info(f"Bank parse complete: {total} transactions across {len(results)} banks")
    return results


def stage_harmonize(logger):
    logger.info("=" * 60)
    logger.info("STAGE 3 — Harmonize Records")
    pid_df, bank_df = load_and_harmonize()
    logger.info(f"PID: {len(pid_df)} rows | Bank: {len(bank_df)} rows")
    return pid_df, bank_df


def stage_canonicalize(pid_df, bank_df, logger):
    logger.info("=" * 60)
    logger.info("STAGE 3b — Fuzzy Vendor Canonicalization (Claude Haiku)")
    bank_df = add_canonical_vendor_column(bank_df, pid_df)
    matched = (bank_df["canonical_vendor"].fillna("").str.strip() != "").sum()
    logger.info(f"  Bank rows with a canonical_vendor: {matched}/{len(bank_df)}")
    return bank_df


def stage_match(pid_df, bank_df, logger):
    logger.info("=" * 60)
    logger.info("STAGE 4 — Deterministic Matching")
    match_df = run_matching(pid_df, bank_df)
    counts = match_df["match_type"].value_counts().to_dict()
    for t, n in counts.items():
        logger.info(f"  {t}: {n}")
    return match_df


def stage_report(pid_df, bank_df, match_df, logger):
    logger.info("=" * 60)
    logger.info("STAGE 5 — Generate Reports")
    reconciled_path, unmatched_path = generate_report(pid_df, bank_df, match_df)
    logger.info(f"  reconciled → {reconciled_path}")
    logger.info(f"  unmatched  → {unmatched_path}")
    return reconciled_path, unmatched_path


def stage_fuzzy_match(pid_df, bank_df, match_df, logger):
    """Report fuzzy vendor matching outputs."""
    logger.info("=" * 60)
    logger.info("STAGE 6 — Fuzzy Vendor Matching")
    fuzzy = match_df[match_df["match_type"] == "fuzzy"]
    logger.info(f"  Fuzzy matches: {len(fuzzy)}")
    if len(fuzzy) > 0:
        logger.info("\n" + fuzzy[["pid_id", "bank_id", "match_confidence", "notes"]].head(10).to_string(index=False))
    return fuzzy


def stage_llm_parse_fallback(args, logger):
    """Run parse stage and summarize LLM fallback extraction stats."""
    logger.info("=" * 60)
    logger.info("STAGE 7 — LLM PDF Parse Fallback")
    bank_dir = Path(args.banks) if args.banks else DATA_BANK_RAW
    _, metadata = parse_banks_run(
        bank_dir,
        return_metadata=True,
        workers=args.bank_parse_workers,
        llm_page_workers=args.llm_page_workers,
        llm_page_max_retries=args.llm_page_max_retries,
        llm_page_retry_base_seconds=args.llm_page_retry_base_seconds,
    )
    files = metadata.get("files", [])
    pages = sum(int((f.get("llm_stats", {}) or {}).get("pages_processed", 0) or 0) for f in files)
    extracted = sum(int((f.get("llm_stats", {}) or {}).get("rows_extracted", 0) or 0) for f in files)
    new_rows = sum(int((f.get("llm_stats", {}) or {}).get("new_records", 0) or 0) for f in files)
    logger.info(f"  Files scanned: {metadata.get('total_files', 0)}")
    logger.info(f"  LLM pages processed: {pages}")
    logger.info(f"  LLM rows extracted: {extracted}")
    logger.info(f"  LLM new rows added: {new_rows}")
    return metadata


def stage_packet_reasoning(pid_df, bank_df, match_df, logger):
    """Generate and persist LLM audit reasoning for all matched records."""
    logger.info("=" * 60)
    logger.info("STAGE 8 — Packet Reasoning")
    matched_records = [r for r in match_df.to_dict("records") if r.get("match_type") != "unmatched"]
    pid_idx = {str(r["pid_id"]): r for _, r in pid_df.iterrows()}
    bank_idx = {str(r["bank_id"]): r for _, r in bank_df.iterrows()}
    reasoning = batch_generate_reasoning(
        matched_records=matched_records,
        pid_df_idx=pid_idx,
        bank_df_idx=bank_idx,
        cache_path=OUTPUT_DIR / "packet_reasoning_cache.json",
    )
    out_path = OUTPUT_DIR / "packet_reasoning.json"
    out_path.write_text(json.dumps(reasoning, indent=2))
    logger.info(f"  Generated reasoning entries: {len(reasoning)}")
    logger.info(f"  reasoning output → {out_path}")
    return reasoning


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Invoice Reconciliation Pipeline"
    )
    parser.add_argument(
        "--pid",
        type=str,
        default=None,
        help="Path to PID file (.xlsx or .csv) — default: first file in data/pid_raw/",
    )
    parser.add_argument(
        "--banks",
        type=str,
        default=None,
        help="Path to bank PDFs directory (default: data/bank_raw/)",
    )
    parser.add_argument(
        "--stage",
        type=str,
        choices=[
            "parse_pid", "parse_banks", "harmonize", "match", "report", "all",
            # Optional explicit stages
            "fuzzy_match", "llm_parse_fallback", "packet_reasoning",
        ],
        default="all",
        help="Run a specific stage (default: all)",
    )
    parser.add_argument("--log-level", type=str, default="INFO")
    parser.add_argument(
        "--bank-parse-workers",
        type=int,
        default=BANK_PARSE_WORKERS,
        help="Process workers for parsing PDFs in parallel.",
    )
    parser.add_argument(
        "--llm-page-workers",
        type=int,
        default=LLM_PAGE_WORKERS,
        help="In-flight page OCR requests per PDF during LLM fallback.",
    )
    parser.add_argument(
        "--llm-page-max-retries",
        type=int,
        default=LLM_PAGE_MAX_RETRIES,
        help="Retries per page OCR request on transient LLM failures.",
    )
    parser.add_argument(
        "--llm-page-retry-base-seconds",
        type=float,
        default=LLM_PAGE_RETRY_BASE_SECONDS,
        help="Base seconds for linear backoff between LLM page retries.",
    )
    args = parser.parse_args()

    logger = setup_logging(level=args.log_level)
    ensure_dirs()

    logger.info("Invoice Reconciliation Pipeline — START")
    logger.info(f"Stage: {args.stage}")
    logger.info(
        "Parse tuning: "
        f"bank_parse_workers={args.bank_parse_workers}, "
        f"llm_page_workers={args.llm_page_workers}, "
        f"llm_page_max_retries={args.llm_page_max_retries}, "
        f"llm_page_retry_base_seconds={args.llm_page_retry_base_seconds}"
    )

    try:
        if args.stage in ("parse_pid", "all"):
            stage_parse_pid(args, logger)

        if args.stage in ("parse_banks", "all"):
            stage_parse_banks(args, logger)

        if args.stage in ("harmonize", "match", "report", "fuzzy_match", "packet_reasoning", "all"):
            pid_df, bank_df = stage_harmonize(logger)

            # Always run vendor canonicalization before matching (idempotent — uses cache)
            if args.stage in ("match", "report", "fuzzy_match", "packet_reasoning", "all"):
                bank_df = stage_canonicalize(pid_df, bank_df, logger)

            if args.stage in ("match", "report", "fuzzy_match", "packet_reasoning", "all"):
                match_df = stage_match(pid_df, bank_df, logger)

                if args.stage in ("report", "all"):
                    stage_report(pid_df, bank_df, match_df, logger)

                if args.stage == "fuzzy_match":
                    stage_fuzzy_match(pid_df, bank_df, match_df, logger)

                if args.stage == "packet_reasoning":
                    stage_packet_reasoning(pid_df, bank_df, match_df, logger)

        if args.stage == "llm_parse_fallback":
            stage_llm_parse_fallback(args, logger)

        logger.info("=" * 60)
        logger.info("Pipeline complete.")

    except FileNotFoundError as e:
        logger.error(f"Missing required file: {e}")
        logger.error("Drop data into data/pid_raw/ and data/bank_raw/, then re-run.")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
