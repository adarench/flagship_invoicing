from __future__ import annotations
"""
src/ingest/parse_bank_pdf.py

For each PDF in data/bank_raw/:
  1. Parse filename → bank name + statement month
  2. Extract transactions via pdfplumber (native text tables)
  3. Normalize to canonical bank schema
  4. Output → data/interim/bank_<BANK>.csv

Bank schema:
    bank_id         str         e.g. "FLAG_2025-07_0001"
    posted_date     date
    check_no        str
    amount          float       raw sign preserved (debits negative)
    description     str
    statement_month str         "YYYY-MM"

Fallback to Claude (MVP 2+) when pdfplumber yields no transactions.
"""

import re
import sys
import logging
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
import pdfplumber
import fitz  # PyMuPDF — used to detect scanned pages for LLM fallback

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import (
    DATA_BANK_RAW,
    DATA_INTERIM,
    BANKS,
    BANK_PARSE_WORKERS,
    LLM_PAGE_WORKERS,
    LLM_PAGE_MAX_RETRIES,
    LLM_PAGE_RETRY_BASE_SECONDS,
)

logger = logging.getLogger(__name__)

MONTH_MAP: dict[str, str] = {
    "jan": "01", "january": "01",
    "feb": "02", "february": "02",
    "mar": "03", "march": "03",
    "apr": "04", "april": "04",
    "may": "05",
    "jun": "06", "june": "06",
    "jul": "07", "july": "07",
    "aug": "08", "august": "08",
    "sep": "09", "sept": "09", "september": "09",
    "oct": "10", "october": "10",
    "nov": "11", "november": "11",
    "dec": "12", "december": "12",
}


# ─── Filename Parsing ────────────────────────────────────────────────────────

def parse_filename(pdf_path: Path) -> tuple[str, str]:
    """
    Extract (bank, statement_month) from PDF filename.

    Handles multiple formats:
      - FLAG_Jul_2025.pdf         → ("FLAG", "2025-07")   standard format
      - FLAGBOROUGH 2025 BANK.pdf → ("FLAG", "2025")       year-only fallback
      - AG_Jan_2025.pdf           → ("AG",   "2025-01")
    """
    stem = pdf_path.stem

    # Tokenise by both underscores AND spaces so both naming conventions work
    tokens = re.split(r"[_\s]+", stem)

    bank  = None
    month = None
    year  = None

    for token in tokens:
        upper = token.upper()
        lower = token.lower()

        # Exact bank ID match
        if upper in BANKS:
            bank = upper
            continue

        # Month name
        if lower in MONTH_MAP:
            month = MONTH_MAP[lower]
            continue

        # 4-digit year
        if re.match(r"^\d{4}$", token):
            year = token
            continue

    # Prefix match for bank (e.g. "FLAGBOROUGH" → FLAG)
    if not bank:
        for b in BANKS:
            if stem.upper().startswith(b) or any(t.upper().startswith(b) for t in tokens):
                bank = b
                break

    if not bank:
        logger.warning(f"Could not identify bank from filename: {pdf_path.name}")
        bank = "UNKNOWN"

    if year and month:
        statement_month = f"{year}-{month}"
    elif year:
        # Month not in filename (e.g. annual statement file)
        statement_month = year
        logger.info(f"No month found in filename '{pdf_path.name}' — using year: {year}")
    else:
        logger.warning(f"Could not parse date from filename: {pdf_path.name}")
        statement_month = "UNKNOWN"

    return bank, statement_month


# ─── Amount / Date Parsers ────────────────────────────────────────────────────

def _parse_amount(val) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s or s in ("-", "–", "—", ""):
        return None
    negative = s.startswith("(") and s.endswith(")")
    s = s.strip("()")
    s = re.sub(r"[$,\s]", "", s)
    # Handle CR/DB suffixes used by some bank exports
    if s.upper().endswith("CR"):
        s = s[:-2]
        negative = False
    elif s.upper().endswith("DB"):
        s = s[:-2]
        negative = True
    try:
        result = float(s)
        return -result if negative else result
    except ValueError:
        return None


def _parse_date(val, year_hint: str | None = None) -> pd.Timestamp | None:
    if not val or str(val).strip().lower() in ("", "nan", "none"):
        return None
    s = str(val).strip()
    # Append year when date has only M/D (e.g. "7/15")
    if year_hint and re.match(r"^\d{1,2}/\d{1,2}$", s):
        s = f"{s}/{year_hint}"
    try:
        return pd.to_datetime(s, infer_datetime_format=True)
    except Exception:
        return None


# ─── Table Column Detection ───────────────────────────────────────────────────

def _detect_columns(headers: list[str]) -> dict[str, int]:
    """
    Map semantic fields to column indices from a header row.
    Returns dict with keys: date, check_no, description, amount, debit, credit, balance
    """
    mapping: dict[str, int] = {}
    for i, h in enumerate(headers):
        h_low = str(h).lower().strip()
        if not h_low:
            continue
        if any(k in h_low for k in ["date", "dt", "posted"]):
            mapping.setdefault("date", i)
        elif any(k in h_low for k in ["check", "chk", "ck #", "check no", "check number", "check#"]):
            mapping.setdefault("check_no", i)
        elif any(k in h_low for k in ["description", "desc", "memo", "payee", "reference", "transaction", "narrative"]):
            mapping.setdefault("description", i)
        elif any(k in h_low for k in ["debit", "withdrawal", "payment", "dr", "paid out"]):
            mapping.setdefault("debit", i)
        elif any(k in h_low for k in ["credit", "deposit", "cr", "paid in"]):
            mapping.setdefault("credit", i)
        elif any(k in h_low for k in ["amount", "amt"]):
            mapping.setdefault("amount", i)
        elif any(k in h_low for k in ["balance", "bal", "running"]):
            mapping.setdefault("balance", i)
    return mapping


# ─── Table → Transaction Rows ─────────────────────────────────────────────────

def _parse_table(
    table: list[list],
    bank: str,
    statement_month: str,
    row_counter: int,
) -> tuple[list[dict], int]:
    """
    Extract transaction dicts from one pdfplumber table.
    Returns (records, updated_row_counter).
    """
    if not table or len(table) < 2:
        return [], row_counter

    year_hint = statement_month[:4] if len(statement_month) >= 4 else None

    # Find header row (first row with any non-empty cell)
    header_row = None
    data_start = 0
    for i, row in enumerate(table):
        if any(c is not None and str(c).strip() for c in row):
            header_row = [str(h) if h is not None else "" for h in row]
            data_start = i + 1
            break

    if header_row is None:
        return [], row_counter

    col_map = _detect_columns(header_row)

    if "date" not in col_map:
        # No date column found — likely not a transaction table
        return [], row_counter

    records: list[dict] = []
    for row in table[data_start:]:
        if not row or all(c is None or str(c).strip() == "" for c in row):
            continue

        # Date must be parseable for a row to be a transaction
        date_val = row[col_map["date"]] if "date" in col_map and col_map["date"] < len(row) else None
        posted_date = _parse_date(date_val, year_hint)
        if posted_date is None:
            continue

        # Check number
        check_raw = row[col_map["check_no"]] if "check_no" in col_map and col_map["check_no"] < len(row) else None
        check_str = str(check_raw).strip() if check_raw else ""
        if check_str.endswith(".0"):
            check_str = check_str[:-2]

        # Description
        desc_raw = row[col_map["description"]] if "description" in col_map and col_map["description"] < len(row) else None
        description = str(desc_raw).strip() if desc_raw else ""

        # Amount — prefer debit/credit split columns, fall back to single amount
        amount: float | None = None
        if "debit" in col_map and col_map["debit"] < len(row) and row[col_map["debit"]]:
            amt = _parse_amount(row[col_map["debit"]])
            if amt:
                amount = -abs(amt)   # debits are outflows (negative)
        elif "credit" in col_map and col_map["credit"] < len(row) and row[col_map["credit"]]:
            amt = _parse_amount(row[col_map["credit"]])
            if amt:
                amount = abs(amt)    # credits are inflows (positive)
        elif "amount" in col_map and col_map["amount"] < len(row):
            amount = _parse_amount(row[col_map["amount"]])

        if amount is None:
            continue

        records.append({
            "bank_id":         f"{bank}_{statement_month}_{row_counter:04d}",
            "posted_date":     posted_date,
            "check_no":        check_str,
            "amount":          amount,
            "description":     description,
            "statement_month": statement_month,
        })
        row_counter += 1

    return records, row_counter


# ─── pdfplumber Extraction: Strategy 1 — Table Extraction ────────────────────

def extract_with_pdfplumber(
    pdf_path: Path, bank: str, statement_month: str
) -> list[dict]:
    """Extract all transactions from PDF using pdfplumber table extraction."""
    all_records: list[dict] = []
    row_counter = 0

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, 1):
            tables = page.extract_tables()
            if not tables:
                logger.debug(f"  Page {page_num}: no tables found")
                continue
            for table in tables:
                records, row_counter = _parse_table(table, bank, statement_month, row_counter)
                if records:
                    logger.debug(f"  Page {page_num}: {len(records)} transactions")
                    all_records.extend(records)

    return all_records


# ─── pdfplumber Extraction: Strategy 2 — Text + Regex (check register layout) ─

# Matches:  MM-DD   CHECKNO   [optional " marker]   AMOUNT
# Handles:  normal entries  "12-08 984 375,757.20"
#           prior-period     "06-02 734 \" 1,736,780.48-"
#           garbled amounts  "12-03 988 20,359.1F" → accepts 1-2 decimal chars
_CHECK_ENTRY_RE = re.compile(
    r"\b(\d{1,2})-(\d{2})\s+(\d{3,6})[\s\"*]{1,5}([\d,]{1,12}\.\d{1,2})",
    re.MULTILINE,
)
# Matches recon page month header, e.g. "MONTH Dec-25"
_MONTH_RE = re.compile(r"MONTH\s+([A-Za-z]{3})-(\d{2})", re.IGNORECASE)


def _build_page_month_map(pdf) -> dict[int, str]:
    """
    Scan all pages for bank reconciliation headers ("MONTH Dec-25").
    Returns mapping: page_number → "YYYY-MM" for every recon page.
    Recon month applies until the next recon page.
    """
    page_month: dict[int, str] = {}
    for i, page in enumerate(pdf.pages, 1):
        text = page.extract_text() or ""
        m = _MONTH_RE.search(text)
        if m:
            mon_str, yr_str = m.group(1).lower(), m.group(2)
            if mon_str in MONTH_MAP:
                year = f"20{yr_str}"
                page_month[i] = f"{year}-{MONTH_MAP[mon_str]}"
    return page_month


def _resolve_month(page_num: int, page_month_map: dict[int, str], fallback: str) -> str:
    """Return the nearest preceding (or equal) recon month for this page."""
    for p in sorted(page_month_map.keys(), reverse=True):
        if p <= page_num:
            return page_month_map[p]
    return fallback


# Matches non-check description entries: MM-DD  TEXT  AMOUNT-
# e.g.: 12-11 Debit Transfer FUND TRF#02736384 INTERN 150,000.00 -
_DESC_ENTRY_RE = re.compile(
    r"\b(\d{1,2})-(\d{2})\s+((?:Debit|Credit|Wire|ACH|Transfer|Fee|Interest|Deposit)"
    r"[A-Za-z0-9\s#/\.\-]{5,80}?)\s{2,}([\d,]+\.\d{2})",
    re.IGNORECASE | re.MULTILINE,
)


def extract_with_text_regex(
    pdf_path: Path, bank: str, statement_month_fallback: str
) -> tuple[list[dict], dict]:
    """
    Strategy 2: pdfplumber text extraction + regex.

    Targets Northern Trust–style check register pages:
      Date  Check No.  Amount  |  Date  Check No.  Amount
    Also captures Description entries (Debit Transfer, ACH, etc.).
    Month is determined per-page from the nearest reconciliation header.

    Returns (records, stats) where stats contains extraction counts.
    """
    all_records: list[dict] = []
    row_counter = 0
    stats = {
        "pages_scanned":       0,
        "check_register_pages": 0,
        "check_entries":        0,
        "desc_entries":         0,
    }

    with pdfplumber.open(pdf_path) as pdf:
        stats["pages_scanned"] = len(pdf.pages)
        page_month_map = _build_page_month_map(pdf)
        stats["page_month_map"] = page_month_map  # exposed for LLM fallback

        for page_num, page in enumerate(pdf.pages, 1):
            text = page.extract_text() or ""
            is_check_page = bool(re.search(r"Date\s+Check\s+N", text, re.IGNORECASE))
            if not is_check_page:
                continue

            stats["check_register_pages"] += 1
            page_sm = _resolve_month(page_num, page_month_map, statement_month_fallback)
            year = page_sm[:4] if len(page_sm) >= 4 else statement_month_fallback[:4]

            page_records = []

            # ── Check entries ─────────────────────────────────────────────────
            for m in _CHECK_ENTRY_RE.finditer(text):
                mon_str, day_str, check_raw, amt_raw = m.groups()
                try:
                    posted_date = pd.to_datetime(f"{year}-{mon_str}-{day_str}")
                except Exception:
                    continue
                try:
                    amount = -abs(float(amt_raw.replace(",", "")))
                except ValueError:
                    continue

                page_records.append({
                    "bank_id":         f"{bank}_{page_sm}_{row_counter:04d}",
                    "posted_date":     posted_date,
                    "check_no":        check_raw.strip(),
                    "amount":          amount,
                    "description":     "",
                    "statement_month": page_sm,
                })
                row_counter += 1
                stats["check_entries"] += 1

            # ── Description entries (Debit Transfer / ACH / Wire) ─────────────
            for m in _DESC_ENTRY_RE.finditer(text):
                mon_str, day_str, desc_raw, amt_raw = m.groups()
                try:
                    posted_date = pd.to_datetime(f"{year}-{mon_str}-{day_str}")
                except Exception:
                    continue
                try:
                    amount = -abs(float(amt_raw.replace(",", "")))
                except ValueError:
                    continue
                desc_clean = re.sub(r"\s{2,}", " ", desc_raw).strip()

                page_records.append({
                    "bank_id":         f"{bank}_{page_sm}_{row_counter:04d}",
                    "posted_date":     posted_date,
                    "check_no":        "",
                    "amount":          amount,
                    "description":     desc_clean,
                    "statement_month": page_sm,
                })
                row_counter += 1
                stats["desc_entries"] += 1

            if page_records:
                logger.debug(
                    f"  Page {page_num} ({page_sm}): "
                    f"{len(page_records)} entries via text regex"
                )
                all_records.extend(page_records)

    return all_records, stats


# ─── Main PDF Parser ──────────────────────────────────────────────────────────

def _sort_bank_frame(df: pd.DataFrame) -> pd.DataFrame:
    sort_cols = [c for c in ["posted_date", "statement_month", "check_no", "bank_id"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, kind="mergesort", na_position="last")
    return df.reset_index(drop=True)


def _parse_pdf_worker(
    pdf_path_str: str,
    llm_page_workers: int,
    llm_page_max_retries: int,
    llm_page_retry_base_seconds: float,
) -> tuple[str, dict]:
    """
    Worker entrypoint for per-PDF raw extraction only.
    Returns (pdf_path_str, raw_payload).
    """
    pdf_path = Path(pdf_path_str)
    raw_payload = _parse_bank_pdf_raw_payload(
        pdf_path=pdf_path,
        llm_page_workers=llm_page_workers,
        llm_page_max_retries=llm_page_max_retries,
        llm_page_retry_base_seconds=llm_page_retry_base_seconds,
    )
    return pdf_path_str, raw_payload


def _parse_bank_pdf_raw_payload(
    pdf_path: Path,
    llm_page_workers: int = LLM_PAGE_WORKERS,
    llm_page_max_retries: int = LLM_PAGE_MAX_RETRIES,
    llm_page_retry_base_seconds: float = LLM_PAGE_RETRY_BASE_SECONDS,
) -> dict:
    """
    Parse one PDF into deterministic raw components.
    No LLM merge/enrichment is applied here.
    """
    bank, statement_month = parse_filename(pdf_path)
    logger.info(f"─── {pdf_path.name}")
    logger.info(f"    bank={bank}  month={statement_month}")

    parse_started = time.perf_counter()
    strategy1_seconds = 0.0
    strategy2_seconds = 0.0
    llm_seconds = 0.0
    records: list[dict] = []
    stats: dict = {}
    llm_stats: dict = {}
    llm_page_outputs: list[dict] = []

    # Strategy 1: structured table extraction
    strategy_started = time.perf_counter()
    records = extract_with_pdfplumber(pdf_path, bank, statement_month)
    strategy1_seconds = time.perf_counter() - strategy_started

    # Strategy 2: text regex (check register layout)
    if not records:
        logger.info("    Strategy 1 (tables): 0 rows — trying text regex")
        strategy_started = time.perf_counter()
        records, stats = extract_with_text_regex(pdf_path, bank, statement_month)
        strategy2_seconds = time.perf_counter() - strategy_started
        logger.info(
            f"    Pages scanned:          {stats.get('pages_scanned', '?')}\n"
            f"    Check-register pages:   {stats.get('check_register_pages', '?')}\n"
            f"    Check entries found:    {stats.get('check_entries', '?')}\n"
            f"    Description entries:    {stats.get('desc_entries', '?')}\n"
            f"    Total extracted:        {len(records)}"
        )
    else:
        logger.info(f"    Strategy 1 (tables): {len(records)} rows")

    # Strategy 3: LLM page extraction (raw page outputs only).
    try:
        from src.ingest.llm_pdf_fallback import extract_llm_fallback_page_outputs
        page_month_map = stats.get("page_month_map", {})
        llm_started = time.perf_counter()
        llm_page_outputs, llm_stats = extract_llm_fallback_page_outputs(
            pdf_path=pdf_path,
            statement_month_fallback=statement_month,
            page_month_map=page_month_map,
            llm_page_workers=llm_page_workers,
            llm_max_retries=llm_page_max_retries,
            llm_retry_base_seconds=llm_page_retry_base_seconds,
        )
        llm_seconds = time.perf_counter() - llm_started
        logger.info(
            f"    LLM fallback raw pages: {llm_stats.get('pages_processed', 0)} "
            f"(rows_extracted={llm_stats.get('rows_extracted', 0)})"
        )
    except ImportError:
        logger.debug("    LLM fallback module not available — skipping")
    except Exception as exc:
        logger.warning(f"    LLM fallback raised an error — skipping: {exc}")

    parse_only_seconds = time.perf_counter() - parse_started
    base_strategy = "tables" if stats == {} else "regex_plus_llm"

    return {
        "filename": pdf_path.name,
        "bank": bank,
        "statement_month": statement_month,
        "base_records": records,
        "strategy": base_strategy,
        "regex_stats": stats,
        "llm_stats": llm_stats,
        "llm_page_outputs": llm_page_outputs,
        "timing": {
            "strategy1_seconds": round(strategy1_seconds, 3),
            "strategy2_seconds": round(strategy2_seconds, 3),
            "llm_seconds": round(llm_seconds, 3),
            "parse_only_seconds": round(parse_only_seconds, 3),
        },
    }


def _finalize_pdf_payload(raw_payload: dict) -> tuple[pd.DataFrame, dict]:
    """
    Deterministically merge raw LLM page outputs into base rows in parent process.
    """
    from src.ingest.llm_pdf_fallback import merge_llm_page_outputs

    filename = str(raw_payload.get("filename", "unknown.pdf"))
    bank = str(raw_payload.get("bank", "UNKNOWN"))
    statement_month = str(raw_payload.get("statement_month", "UNKNOWN"))
    base_records = [dict(r) for r in (raw_payload.get("base_records", []) or [])]
    llm_page_outputs = raw_payload.get("llm_page_outputs", []) or []
    regex_stats = raw_payload.get("regex_stats", {}) or {}
    llm_stats_raw = raw_payload.get("llm_stats", {}) or {}
    timing = raw_payload.get("timing", {}) or {}
    merge_started = time.perf_counter()
    llm_new_rows, merge_stats = merge_llm_page_outputs(
        pdf_filename=filename,
        bank=bank,
        existing_rows=base_records,
        page_outputs=llm_page_outputs,
        row_counter_start=len(base_records),
    )
    merge_seconds = time.perf_counter() - merge_started

    llm_stats = {
        **llm_stats_raw,
        **merge_stats,
    }
    records = base_records + llm_new_rows

    if not records:
        logger.warning(f"    All extraction strategies found no transactions in {filename}.")
        total_seconds = float(timing.get("parse_only_seconds", 0.0) or 0.0) + merge_seconds
        empty_df = pd.DataFrame(
            columns=["bank_id", "posted_date", "check_no", "amount", "description", "statement_month"]
        )
        meta = {
            "filename": filename,
            "bank": bank,
            "statement_month": statement_month,
            "rows_extracted": 0,
            "strategy": "none",
            "regex_stats": regex_stats,
            "llm_stats": llm_stats,
            "parse_seconds": round(total_seconds, 3),
            "timing": {
                "strategy1_seconds": float(timing.get("strategy1_seconds", 0.0) or 0.0),
                "strategy2_seconds": float(timing.get("strategy2_seconds", 0.0) or 0.0),
                "llm_seconds": float(timing.get("llm_seconds", 0.0) or 0.0),
                "merge_seconds": round(merge_seconds, 3),
                "total_seconds": round(total_seconds, 3),
            },
        }
        return empty_df, meta

    df = pd.DataFrame(records)
    core_cols = ["bank_id", "posted_date", "check_no", "amount", "description", "statement_month"]
    extra_cols = [c for c in df.columns if c not in core_cols]
    df = df[core_cols + extra_cols]

    llm_new = int(llm_stats.get("new_records", 0) or 0)
    logger.info(
        f"  Extracted {len(df)} transactions "
        f"(regex: {len(df) - llm_new}, llm_new: {llm_new})"
    )

    total_seconds = float(timing.get("parse_only_seconds", 0.0) or 0.0) + merge_seconds
    strategy = str(raw_payload.get("strategy", "regex_plus_llm"))
    meta = {
        "filename": filename,
        "bank": bank,
        "statement_month": statement_month,
        "rows_extracted": len(df),
        "strategy": strategy,
        "regex_stats": regex_stats,
        "llm_stats": llm_stats,
        "parse_seconds": round(total_seconds, 3),
        "timing": {
            "strategy1_seconds": float(timing.get("strategy1_seconds", 0.0) or 0.0),
            "strategy2_seconds": float(timing.get("strategy2_seconds", 0.0) or 0.0),
            "llm_seconds": float(timing.get("llm_seconds", 0.0) or 0.0),
            "merge_seconds": round(merge_seconds, 3),
            "total_seconds": round(total_seconds, 3),
        },
    }
    return df, meta


def parse_bank_pdf(
    pdf_path: Path,
    return_meta: bool = False,
    llm_page_workers: int = LLM_PAGE_WORKERS,
    llm_page_max_retries: int = LLM_PAGE_MAX_RETRIES,
    llm_page_retry_base_seconds: float = LLM_PAGE_RETRY_BASE_SECONDS,
) -> pd.DataFrame | tuple[pd.DataFrame, dict]:
    """
    Parse a single bank statement PDF → normalized DataFrame.

    Extraction strategy (in order):
      1. pdfplumber table extraction (works for digitally-generated PDFs)
      2. pdfplumber text + regex (works for Northern Trust check register layout)
      3. LLM fallback for scanned check-face pages
    """
    raw_payload = _parse_bank_pdf_raw_payload(
        pdf_path=pdf_path,
        llm_page_workers=llm_page_workers,
        llm_page_max_retries=llm_page_max_retries,
        llm_page_retry_base_seconds=llm_page_retry_base_seconds,
    )
    df, meta = _finalize_pdf_payload(raw_payload)
    if return_meta:
        return df, meta
    return df


# ─── Batch Runner ─────────────────────────────────────────────────────────────

def run(
    bank_raw_dir: Path | None = None,
    interim_dir: Path | None = None,
    return_metadata: bool = False,
    workers: int | None = None,
    llm_page_workers: int = LLM_PAGE_WORKERS,
    llm_page_max_retries: int = LLM_PAGE_MAX_RETRIES,
    llm_page_retry_base_seconds: float = LLM_PAGE_RETRY_BASE_SECONDS,
) -> dict[str, pd.DataFrame] | tuple[dict[str, pd.DataFrame], dict]:
    """
    Parse all PDFs in data/bank_raw/.
    Groups by bank, saves data/interim/bank_<BANK>.csv.
    Returns dict: bank_name → DataFrame.
    """
    if bank_raw_dir is None:
        bank_raw_dir = DATA_BANK_RAW
    if interim_dir is None:
        interim_dir = DATA_INTERIM

    pdfs = sorted(bank_raw_dir.glob("*.pdf"))
    if not pdfs:
        logger.warning(f"No PDF files found in {bank_raw_dir}")
        empty: dict[str, pd.DataFrame] = {}
        if return_metadata:
            return empty, {"files": [], "total_files": 0, "timing": {"total_seconds": 0.0}}
        return empty

    parse_started = time.perf_counter()
    configured_workers = workers if workers is not None else BANK_PARSE_WORKERS
    effective_workers = max(1, min(int(configured_workers), len(pdfs)))

    logger.info(
        f"Bank parse starting: files={len(pdfs)}, "
        f"pdf_workers={effective_workers}, llm_page_workers={llm_page_workers}"
    )

    bank_frames: dict[str, list[pd.DataFrame]] = {}
    parse_meta: list[dict] = []
    raw_by_pdf: dict[str, dict] = {}

    if effective_workers <= 1 or len(pdfs) <= 1:
        for pdf_path in pdfs:
            raw_payload = _parse_bank_pdf_raw_payload(
                pdf_path=pdf_path,
                llm_page_workers=llm_page_workers,
                llm_page_max_retries=llm_page_max_retries,
                llm_page_retry_base_seconds=llm_page_retry_base_seconds,
            )
            raw_by_pdf[str(pdf_path)] = raw_payload
    else:
        try:
            with ProcessPoolExecutor(max_workers=effective_workers) as pool:
                future_to_pdf = {
                    pool.submit(
                        _parse_pdf_worker,
                        str(pdf_path),
                        llm_page_workers,
                        llm_page_max_retries,
                        llm_page_retry_base_seconds,
                    ): pdf_path
                    for pdf_path in pdfs
                }
                for fut in as_completed(future_to_pdf):
                    pdf_path = future_to_pdf[fut]
                    try:
                        pdf_path_str, raw_payload = fut.result()
                        raw_by_pdf[pdf_path_str] = raw_payload
                    except Exception as exc:
                        logger.warning(
                            f"Parallel worker failed for {pdf_path.name}; "
                            f"retrying once in-process: {exc}"
                        )
                        try:
                            raw_payload = _parse_bank_pdf_raw_payload(
                                pdf_path=pdf_path,
                                llm_page_workers=llm_page_workers,
                                llm_page_max_retries=llm_page_max_retries,
                                llm_page_retry_base_seconds=llm_page_retry_base_seconds,
                            )
                            raw_payload["worker_retry"] = True
                            raw_by_pdf[str(pdf_path)] = raw_payload
                        except Exception as retry_exc:
                            raise RuntimeError(
                                f"Failed to parse {pdf_path.name} after retry"
                            ) from retry_exc
        except Exception as pool_exc:
            logger.warning(
                f"Parallel parse pool unavailable ({pool_exc}); "
                "falling back to sequential parsing"
            )
            for pdf_path in pdfs:
                key = str(pdf_path)
                if key in raw_by_pdf:
                    continue
                raw_payload = _parse_bank_pdf_raw_payload(
                    pdf_path=pdf_path,
                    llm_page_workers=llm_page_workers,
                    llm_page_max_retries=llm_page_max_retries,
                    llm_page_retry_base_seconds=llm_page_retry_base_seconds,
                )
                raw_payload["parallel_fallback"] = True
                raw_by_pdf[key] = raw_payload

    # Collate in deterministic PDF filename order.
    for pdf_path in pdfs:
        key = str(pdf_path)
        if key not in raw_by_pdf:
            raise RuntimeError(f"Missing parse result for {pdf_path.name}")
        raw_payload = raw_by_pdf[key]
        df, meta = _finalize_pdf_payload(raw_payload)
        if raw_payload.get("worker_retry"):
            meta["worker_retry"] = True
        if raw_payload.get("parallel_fallback"):
            meta["parallel_fallback"] = True
        bank = str(meta.get("bank", "UNKNOWN"))
        parse_meta.append(meta)
        if not df.empty:
            bank_frames.setdefault(bank, []).append(df)

    result: dict[str, pd.DataFrame] = {}
    for bank, dfs in bank_frames.items():
        combined = pd.concat(dfs, ignore_index=True)
        combined = _sort_bank_frame(combined)
        output_path = interim_dir / f"bank_{bank}.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(output_path, index=False)
        logger.info(f"Saved {len(combined)} transactions → {output_path}")
        result[bank] = combined

    total_seconds = time.perf_counter() - parse_started
    logger.info(
        f"Bank parse complete: files={len(parse_meta)}, banks={len(result)}, "
        f"total_seconds={total_seconds:.2f}"
    )

    if return_metadata:
        return result, {
            "files": parse_meta,
            "total_files": len(parse_meta),
            "timing": {
                "total_seconds": round(total_seconds, 3),
                "pdf_workers": effective_workers,
                "llm_page_workers": llm_page_workers,
                "llm_page_max_retries": llm_page_max_retries,
            },
        }
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    results = run(return_metadata=False)
    for bank, df in results.items():
        print(f"\n{bank}: {len(df)} transactions")
        print(df.head(3).to_string())
