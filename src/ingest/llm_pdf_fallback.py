from __future__ import annotations
"""
src/ingest/llm_pdf_fallback.py

MVP 2: LLM-based OCR for scanned check pages in Northern Trust bank PDFs.

Northern Trust statement structure (per statement period):
  - Page 1 of N: Cover / reconciliation page  (text)
  - Page 2 of N: Check register (Date|Check No.|Amount columns, text)  ← already parsed
  - Pages 3-N:   Individual scanned check face images (1-bit PNG embedded in page)

This module targets pages 3-N (check face images):
  1. Detects them by: has ≥1 large (>20 KB) 1-bit PNG AND no "Date Check N" header
  2. Extracts the embedded 1-bit PNG directly (higher resolution than full-page render)
  3. Converts to RGB for Claude Sonnet vision
  4. Returns structured check fields: check_no, date, amount, vendor, raw_text
  5. Merges into existing rows: enriches description if check_no matches; else adds new row

Public API used by parse_bank_pdf.py:
    records, stats = extract_llm_fallback_rows(
        pdf_path, bank, statement_month_fallback,
        existing_rows, page_month_map, row_counter_start
    )
"""

import base64
import hashlib
import io
import json
import logging
import os
import re
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import fitz           # PyMuPDF
from PIL import Image
import pandas as pd
try:
    import fcntl  # type: ignore
except ImportError:  # pragma: no cover - non-posix fallback
    fcntl = None

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import (
    get_sonnet_client,
    CLAUDE_SONNET,
    LLM_PAGE_WORKERS,
    LLM_PAGE_MAX_RETRIES,
    LLM_PAGE_RETRY_BASE_SECONDS,
)

logger = logging.getLogger(__name__)

# Minimum bytes for an embedded 1-bit PNG to be considered a real check image
_MIN_CHECK_IMAGE_BYTES = 20_000

_LLM_PAGE_CACHE_PATH = Path(__file__).parent.parent.parent / "storage" / "llm_page_cache.json"
_LLM_PAGE_CACHE_LOCK = _LLM_PAGE_CACHE_PATH.with_suffix(".lock")

# Selectable text headers that indicate a page is already parsed by regex/pdfplumber
_CHECK_REGISTER_RE = re.compile(r"Date\s+Check\s+N", re.IGNORECASE)
_RECON_MONTH_RE    = re.compile(r"MONTH\s+[A-Za-z]{3}-\d{2}", re.IGNORECASE)


# ─── Page Cache ────────────────────────────────────────────────────────────────

@contextmanager
def _cache_lock():
    """Cross-process lock for cache read/write."""
    _LLM_PAGE_CACHE_LOCK.parent.mkdir(parents=True, exist_ok=True)
    with open(_LLM_PAGE_CACHE_LOCK, "a+", encoding="utf-8") as lock_file:
        if fcntl is not None:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _read_cache_unlocked() -> dict[str, dict]:
    if not _LLM_PAGE_CACHE_PATH.exists():
        return {}
    try:
        raw = json.loads(_LLM_PAGE_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("LLM page cache is unreadable; starting with empty cache")
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict] = {}
    for k, v in raw.items():
        if isinstance(k, str) and isinstance(v, dict):
            out[k] = v
    return out


def _write_cache_unlocked(cache: dict[str, dict]) -> None:
    _LLM_PAGE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _LLM_PAGE_CACHE_PATH.with_name(f"{_LLM_PAGE_CACHE_PATH.name}.tmp.{os.getpid()}")
    tmp.write_text(json.dumps(cache, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(_LLM_PAGE_CACHE_PATH)


def _load_llm_page_cache() -> dict[str, dict]:
    with _cache_lock():
        return _read_cache_unlocked()


def _upsert_llm_page_cache(entries: dict[str, dict]) -> None:
    if not entries:
        return
    with _cache_lock():
        cache = _read_cache_unlocked()
        cache.update(entries)
        _write_cache_unlocked(cache)


def _normalise_ocr_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _compute_page_hash(pdf_filename: str, page_number: int, ocr_text: str) -> str:
    normalized_text = _normalise_ocr_text(ocr_text)
    payload = f"{pdf_filename}|{page_number}|{normalized_text}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ─── Prompt ───────────────────────────────────────────────────────────────────

_OCR_PROMPT = """\
You are extracting check information from a scanned Northern Trust bank check image.

This image is a scanned photograph of one or more physical bank checks.
For EACH check visible in the image, extract:
  - check_no:   the check number (in the upper-right corner or MICR line at bottom)
  - check_date: the date on the check (normalize to YYYY-MM-DD)
  - amount:     the numeric dollar amount only (e.g. "375757.20"), no $ or commas
  - vendor:     the full payee name on the "Pay to the order of" line
  - raw_text:   any memo / description line text visible

Reply with ONLY a JSON array — one object per check found.
If no readable check is visible, reply with [].
If a specific field cannot be read clearly, use null.
No markdown, no explanation. Just the JSON array.

Example:
[{"check_no": "984", "check_date": "2025-12-08", "amount": "375757.20", "vendor": "CONCRETE CONTRACTING INC", "raw_text": "Pay App 12"}]"""


# ─── Phase 1: Image extraction helpers ───────────────────────────────────────

def _extract_largest_check_image(doc: fitz.Document, page_index: int) -> bytes | None:
    """
    Extract the largest 1-bit PNG from a PDF page as RGB PNG bytes.
    Returns None if no qualifying check image is found.
    """
    page = doc[page_index]
    best_size  = 0
    best_xref  = None

    for img_info in page.get_images(full=True):
        xref = img_info[0]
        d    = doc.extract_image(xref)
        if d.get("bpc") == 1 and len(d["image"]) > _MIN_CHECK_IMAGE_BYTES:
            if len(d["image"]) > best_size:
                best_size = len(d["image"])
                best_xref = xref

    if best_xref is None:
        return None

    raw = doc.extract_image(best_xref)["image"]
    # Convert 1-bit image (black/white) to RGB so Claude handles it correctly
    pil = Image.open(io.BytesIO(raw)).convert("RGB")
    buf = io.BytesIO()
    pil.save(buf, format="PNG")
    return buf.getvalue()


def pdf_page_to_image(pdf_path: str | Path, page_index: int) -> Image.Image:
    """
    Render a single PDF page to a PIL.Image via full-page rasterization.
    Used as fallback when direct image extraction is not available.
    Renders at ~150 DPI.
    """
    scale = 150 / 72.0
    mat   = fitz.Matrix(scale, scale)
    doc   = fitz.open(str(pdf_path))
    page  = doc[page_index]
    pix   = page.get_pixmap(matrix=mat, colorspace=fitz.csRGB)
    doc.close()
    return Image.frombytes("RGB", [pix.width, pix.height], pix.samples)


def image_to_png_bytes(image: Image.Image) -> bytes:
    """Encode a PIL.Image as PNG bytes."""
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    return buf.getvalue()


def _page_image_bytes(doc: fitz.Document, page_index: int) -> bytes | None:
    """
    Get the best available image bytes for a page.
    Prefers direct 1-bit PNG extraction; falls back to full-page render.
    Returns None if neither yields a useful image.
    """
    # Try embedded check image first (better quality, compact)
    img_bytes = _extract_largest_check_image(doc, page_index)
    if img_bytes is not None:
        return img_bytes

    # Fall back to full-page rasterization
    scale = 150 / 72.0
    mat   = fitz.Matrix(scale, scale)
    page  = doc[page_index]
    pix   = page.get_pixmap(matrix=mat, colorspace=fitz.csRGB)
    img   = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

    # Reject mostly-white pages (blank separators)
    import numpy as np
    arr = np.array(img)
    if arr.mean() > 250:
        return None

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


# ─── Phase 2: Claude Sonnet vision OCR ───────────────────────────────────────

def llm_extract_check_fields(image_bytes: bytes) -> list[dict]:
    """
    Send a check page image to Claude Sonnet vision.

    Returns a list of dicts, one per check found:
        check_no   str
        check_date str | None   YYYY-MM-DD
        amount     float | None (negative debit)
        vendor     str
        raw_text   str

    Returns [] on API failure or if no check is visible.
    """
    client = get_sonnet_client()
    b64    = base64.standard_b64encode(image_bytes).decode("utf-8")

    response = client.messages.create(
        model=CLAUDE_SONNET,
        max_tokens=1500,
        temperature=0,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type":       "base64",
                        "media_type": "image/png",
                        "data":       b64,
                    },
                },
                {
                    "type": "text",
                    "text": _OCR_PROMPT,
                },
            ],
        }],
    )

    raw = response.content[0].text.strip()
    raw = re.sub(r"^```[a-z]*\n?", "", raw, flags=re.IGNORECASE).rstrip("`").strip()

    if not raw or raw.lower() in ("null", "none", "[]"):
        return []

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning(f"LLM malformed JSON (first 200 chars): {raw[:200]}")
        return []

    if not isinstance(parsed, list):
        parsed = [parsed] if isinstance(parsed, dict) else []

    return [_normalise_llm_item(item) for item in parsed if isinstance(item, dict)]


def _llm_extract_with_retry(
    image_bytes: bytes,
    max_retries: int,
    retry_base_seconds: float,
) -> tuple[list[dict], int, str | None]:
    """
    Run one page OCR request with transient retry/backoff.
    Returns: (extracted_items, attempts, error_message_or_none)
    """
    attempts = 0
    for attempt_idx in range(max_retries + 1):
        attempts = attempt_idx + 1
        try:
            return llm_extract_check_fields(image_bytes), attempts, None
        except Exception as exc:
            if attempt_idx >= max_retries:
                return [], attempts, str(exc)
            sleep_s = retry_base_seconds * attempts
            logger.warning(
                f"    LLM fallback transient error (attempt {attempts}/{max_retries + 1}) "
                f"— retrying in {sleep_s:.2f}s: {exc}"
            )
            time.sleep(sleep_s)
    return [], attempts, "unexpected retry loop termination"


def _normalise_llm_item(item: dict) -> dict:
    """Clean and type-cast one LLM-extracted check dict."""
    cn_raw   = item.get("check_no")
    check_no = re.sub(r"\s+", "", str(cn_raw).strip()) if cn_raw is not None else ""

    dt_raw     = item.get("check_date")
    check_date = None
    if dt_raw:
        try:
            check_date = pd.to_datetime(str(dt_raw)).strftime("%Y-%m-%d")
        except Exception:
            pass

    amt_raw = item.get("amount")
    amount  = None
    if amt_raw is not None:
        try:
            amount = -abs(float(str(amt_raw).replace(",", "").replace("$", "").strip()))
        except (ValueError, TypeError):
            pass

    vendor   = str(item.get("vendor",   "") or "").strip()
    raw_text = str(item.get("raw_text", "") or "").strip()

    return {
        "check_no":   check_no,
        "check_date": check_date,
        "amount":     amount,
        "vendor":     vendor,
        "raw_text":   raw_text,
    }


# ─── Check-face page detection ───────────────────────────────────────────────

def _is_check_face_page(doc: fitz.Document, page_index: int, page_text: str) -> bool:
    """
    Return True if this page contains a scanned check face image that needs LLM.
    Criteria:
      - Has ≥1 embedded 1-bit PNG > _MIN_CHECK_IMAGE_BYTES
      - Does NOT already contain a text check register ("Date Check N" header)
      - Does NOT contain a month reconciliation header
    """
    if _CHECK_REGISTER_RE.search(page_text):
        return False
    if _RECON_MONTH_RE.search(page_text):
        return False

    page = doc[page_index]
    for img_info in page.get_images(full=True):
        xref = img_info[0]
        d    = doc.extract_image(xref)
        if d.get("bpc") == 1 and len(d["image"]) > _MIN_CHECK_IMAGE_BYTES:
            return True
    return False


# ─── Phase 3 Integration: LLM fallback driver ────────────────────────────────

def _resolve_statement_month_for_page(
    page_1idx: int,
    page_month_map: dict[int, str],
    statement_month_fallback: str,
) -> str:
    for p in sorted(page_month_map.keys(), reverse=True):
        if p <= page_1idx:
            return page_month_map[p]
    return statement_month_fallback


def extract_llm_fallback_page_outputs(
    pdf_path: str | Path,
    statement_month_fallback: str,
    page_month_map: dict[int, str],
    llm_page_workers: int = LLM_PAGE_WORKERS,
    llm_max_retries: int = LLM_PAGE_MAX_RETRIES,
    llm_retry_base_seconds: float = LLM_PAGE_RETRY_BASE_SECONDS,
) -> tuple[list[dict], dict]:
    """
    Extract raw page-level LLM outputs only (no merge/enrichment side effects).
    Returns:
      page_outputs: [{page_number, statement_month, checks:[...]}]
      stats: page/call counters for telemetry
    """
    import pdfplumber

    pdf_path = Path(pdf_path)

    check_face_indices: list[int] = []
    page_text_by_index: dict[int, str] = {}
    page_payloads: list[tuple[int, str, bytes, str, str]] = []
    skipped_no_image = 0
    cache_hits = 0
    cache_misses = 0
    cache_writes = 0
    page_cache = _load_llm_page_cache()
    doc = fitz.open(str(pdf_path))
    try:
        # Collect check-face pages using pdfplumber text + fitz image detection.
        with pdfplumber.open(pdf_path) as ppdf:
            for i, pp in enumerate(ppdf.pages):
                text = pp.extract_text() or ""
                page_text_by_index[i] = text
                if _is_check_face_page(doc, i, text):
                    check_face_indices.append(i)

        logger.info(f"    LLM fallback: {len(check_face_indices)} check-face pages identified")

        # Extract page images while fitz doc is open; OCR requests happen after this.
        for page_0idx in check_face_indices:
            page_1idx = page_0idx + 1
            page_sm = _resolve_statement_month_for_page(
                page_1idx=page_1idx,
                page_month_map=page_month_map,
                statement_month_fallback=statement_month_fallback,
            )
            img_bytes = _page_image_bytes(doc, page_0idx)
            if img_bytes is None:
                skipped_no_image += 1
                logger.debug(f"    Page {page_1idx}: no usable image — skipping")
                continue
            ocr_text = page_text_by_index.get(page_0idx, "")
            page_hash = _compute_page_hash(
                pdf_filename=pdf_path.name,
                page_number=page_1idx,
                ocr_text=ocr_text,
            )
            cached_entry = page_cache.get(page_hash, {})
            if isinstance(cached_entry, dict) and isinstance(cached_entry.get("extracted"), list):
                cache_hits += 1
                page_payloads.append((page_0idx, page_sm, b"", page_hash, "cached"))
            else:
                cache_misses += 1
                page_payloads.append((page_0idx, page_sm, img_bytes, page_hash, "miss"))
    finally:
        doc.close()

    def _ocr_one_page(payload: tuple[int, str, bytes, str, str]) -> dict:
        page_0idx, page_sm, img_bytes, page_hash, cache_status = payload
        if cache_status == "cached":
            cached_entry = page_cache.get(page_hash, {})
            extracted = cached_entry.get("extracted", []) if isinstance(cached_entry, dict) else []
            return {
                "page_0idx": page_0idx,
                "page_sm": page_sm,
                "extracted": extracted if isinstance(extracted, list) else [],
                "attempts": 0,
                "error": None,
                "page_hash": page_hash,
                "from_cache": True,
            }

        extracted, attempts, error = _llm_extract_with_retry(
            image_bytes=img_bytes,
            max_retries=llm_max_retries,
            retry_base_seconds=llm_retry_base_seconds,
        )
        return {
            "page_0idx": page_0idx,
            "page_sm": page_sm,
            "extracted": extracted,
            "attempts": attempts,
            "error": error,
            "page_hash": page_hash,
            "from_cache": False,
        }

    page_results: list[dict] = []
    max_workers = max(1, min(llm_page_workers, len(page_payloads))) if page_payloads else 1
    if page_payloads and max_workers > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_ocr_one_page, payload) for payload in page_payloads]
            for fut in as_completed(futures):
                try:
                    page_results.append(fut.result())
                except Exception as exc:
                    # Defensive guard; _ocr_one_page already captures OCR errors.
                    page_results.append({
                        "page_0idx": -1,
                        "page_sm": statement_month_fallback,
                        "extracted": [],
                        "attempts": llm_max_retries + 1,
                        "error": str(exc),
                        "page_hash": "",
                        "from_cache": False,
                    })
    else:
        for payload in page_payloads:
            page_results.append(_ocr_one_page(payload))

    page_outputs: list[dict] = []
    pages_processed = 0
    rows_extracted = 0
    pages_failed = 0
    retries_used = 0
    cache_updates: dict[str, dict] = {}

    for pres in sorted(page_results, key=lambda r: r.get("page_0idx", -1)):
        page_0idx = int(pres.get("page_0idx", -1))
        page_sm = str(pres.get("page_sm", statement_month_fallback))
        extracted = pres.get("extracted", []) or []
        attempts = int(pres.get("attempts", 0) or 0)
        retries_used += max(0, attempts - 1)

        if pres.get("error"):
            pages_failed += 1
            logger.warning(
                f"    LLM fallback → page {page_0idx + 1}: failed after {attempts} attempt(s) — "
                f"{pres.get('error')}"
            )
            continue

        page_hash = str(pres.get("page_hash", "") or "")
        from_cache = bool(pres.get("from_cache"))
        if page_hash and not from_cache:
            cache_updates[page_hash] = {
                "page_hash": page_hash,
                "extracted": extracted if isinstance(extracted, list) else [],
                "model_version": CLAUDE_SONNET,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

        pages_processed += 1
        checks = [
            {
                "check_no": str((item or {}).get("check_no", "") or "").strip(),
                "check_date": (item or {}).get("check_date"),
                "amount": (item or {}).get("amount"),
                "vendor": str((item or {}).get("vendor", "") or "").strip(),
                "raw_text": str((item or {}).get("raw_text", "") or "").strip(),
            }
            for item in extracted
            if isinstance(item, dict)
        ]
        checks = sorted(
            checks,
            key=lambda item: (
                item.get("check_no", ""),
                str(item.get("check_date", "") or ""),
                float(item.get("amount")) if item.get("amount") is not None else 0.0,
                item.get("vendor", ""),
                item.get("raw_text", ""),
            ),
        )
        rows_extracted += len(checks)
        page_outputs.append(
            {
                "page_number": page_0idx + 1,
                "statement_month": page_sm,
                "checks": checks,
            }
        )
        for item in checks:
            logger.info(
                f"    LLM fallback → page {page_0idx + 1} → "
                f"check_no={item['check_no']!r}  vendor={item['vendor']!r}"
            )

    page_outputs = sorted(page_outputs, key=lambda p: int(p.get("page_number", 0) or 0))
    if cache_updates:
        _upsert_llm_page_cache(cache_updates)
        cache_writes = len(cache_updates)

    stats = {
        "pages_processed": pages_processed,
        "rows_extracted": rows_extracted,
        "pages_identified": len(check_face_indices),
        "pages_failed": pages_failed,
        "pages_skipped_no_image": skipped_no_image,
        "retries_used": retries_used,
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "cache_writes": cache_writes,
    }
    return page_outputs, stats


def merge_llm_page_outputs(
    *,
    pdf_filename: str,
    bank: str,
    existing_rows: list[dict],
    page_outputs: list[dict],
    row_counter_start: int = 0,
) -> tuple[list[dict], dict]:
    """
    Deterministically merge raw page outputs into parsed rows.
    Sort key for merge order: (pdf_filename, page_number, check_no).
    """
    existing_by_check: dict[str, int] = {}
    for idx, row in enumerate(existing_rows):
        cn = str(row.get("check_no", "") or "").strip()
        if cn and cn not in existing_by_check:
            existing_by_check[cn] = idx

    flattened: list[tuple[int, str, dict]] = []
    for page in page_outputs:
        page_number = int(page.get("page_number", 0) or 0)
        statement_month = str(page.get("statement_month", "") or "")
        for item in page.get("checks", []) or []:
            if not isinstance(item, dict):
                continue
            flattened.append((page_number, statement_month, item))

    flattened = sorted(
        flattened,
        key=lambda row: (
            pdf_filename,
            row[0],
            str(row[2].get("check_no", "") or ""),
            str(row[2].get("check_date", "") or ""),
            float(row[2].get("amount")) if row[2].get("amount") is not None else 0.0,
            str(row[2].get("vendor", "") or ""),
            str(row[2].get("raw_text", "") or ""),
        ),
    )

    new_rows: list[dict] = []
    row_counter = row_counter_start
    vendor_matches = 0
    duplicate_new_records_skipped = 0
    seen_new_row_keys: set[tuple] = set()

    for page_number, page_sm, item in flattened:
        cn = str(item.get("check_no", "") or "").strip()
        desc = str(item.get("vendor", "") or "").strip() or str(item.get("raw_text", "") or "").strip()
        amount = item.get("amount")

        if cn and cn in existing_by_check:
            idx = existing_by_check[cn]
            if not str(existing_rows[idx].get("description", "") or "").strip() and desc:
                existing_rows[idx]["description"] = desc
                vendor_matches += 1
            continue

        if amount is None:
            continue

        posted_date = None
        if item.get("check_date"):
            try:
                posted_date = pd.to_datetime(item.get("check_date"))
            except Exception:
                posted_date = None
        if posted_date is None and len(page_sm) == 7:
            try:
                posted_date = pd.to_datetime(f"{page_sm}-01")
            except Exception:
                posted_date = None

        dedupe_key = (
            pdf_filename,
            page_number,
            cn,
            str(item.get("check_date", "") or ""),
            float(amount),
            desc,
        )
        if dedupe_key in seen_new_row_keys:
            duplicate_new_records_skipped += 1
            continue
        seen_new_row_keys.add(dedupe_key)

        new_rows.append(
            {
                "bank_id": f"{bank}_{page_sm}_LLM{row_counter:04d}",
                "posted_date": posted_date,
                "check_no": cn,
                "amount": amount,
                "description": desc,
                "statement_month": page_sm,
                "source": "llm_fallback",
            }
        )
        row_counter += 1

    return new_rows, {
        "vendor_matches": vendor_matches,
        "new_records": len(new_rows),
        "duplicate_new_records_skipped": duplicate_new_records_skipped,
    }


def extract_llm_fallback_rows(
    pdf_path:                 str | Path,
    bank:                     str,
    statement_month_fallback:  str,
    existing_rows:            list[dict],
    page_month_map:           dict[int, str],
    row_counter_start:        int = 0,
    llm_page_workers:         int = LLM_PAGE_WORKERS,
    llm_max_retries:          int = LLM_PAGE_MAX_RETRIES,
    llm_retry_base_seconds:   float = LLM_PAGE_RETRY_BASE_SECONDS,
) -> tuple[list[dict], dict]:
    """
    Backward-compatible API: run raw page extraction then deterministic merge.
    """
    page_outputs, page_stats = extract_llm_fallback_page_outputs(
        pdf_path=pdf_path,
        statement_month_fallback=statement_month_fallback,
        page_month_map=page_month_map,
        llm_page_workers=llm_page_workers,
        llm_max_retries=llm_max_retries,
        llm_retry_base_seconds=llm_retry_base_seconds,
    )
    new_rows, merge_stats = merge_llm_page_outputs(
        pdf_filename=Path(pdf_path).name,
        bank=bank,
        existing_rows=existing_rows,
        page_outputs=page_outputs,
        row_counter_start=row_counter_start,
    )
    stats = {
        **page_stats,
        **merge_stats,
    }
    logger.info(
        f"    LLM fallback complete — "
        f"pages={stats.get('pages_processed', 0)}, extracted={stats.get('rows_extracted', 0)}, "
        f"vendor_enriched={stats.get('vendor_matches', 0)}, new_records={stats.get('new_records', 0)}"
    )
    return new_rows, stats
