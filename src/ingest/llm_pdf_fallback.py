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
import io
import json
import logging
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import fitz           # PyMuPDF
from PIL import Image
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import get_sonnet_client, CLAUDE_SONNET

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Minimum bytes for an embedded 1-bit PNG to be considered a real check image
_MIN_CHECK_IMAGE_BYTES = 20_000

# Selectable text headers that indicate a page is already parsed by regex/pdfplumber
_CHECK_REGISTER_RE = re.compile(r"Date\s+Check\s+N", re.IGNORECASE)
_RECON_MONTH_RE    = re.compile(r"MONTH\s+[A-Za-z]{3}-\d{2}", re.IGNORECASE)


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

def extract_llm_fallback_rows(
    pdf_path:                 str | Path,
    bank:                     str,
    statement_month_fallback:  str,
    existing_rows:            list[dict],
    page_month_map:           dict[int, str],
    row_counter_start:        int = 0,
) -> tuple[list[dict], dict]:
    """
    Run LLM OCR on every check-face page of the PDF.

    Check-face pages are identified as: large embedded 1-bit PNG + no check register header.

    Merge strategy:
      * If check_no matches an existing (regex) row with empty description →
        write vendor name into that row's description field.
      * If check_no not in existing rows → append a new row.

    Args:
        pdf_path:                  Path to the PDF.
        bank:                      Canonical bank ID (e.g. "FLAG").
        statement_month_fallback:  Fallback month when no recon header precedes page.
        existing_rows:             Already-extracted rows from regex/pdfplumber.
                                   Descriptions are updated in-place.
        page_month_map:            {1-based page_no → "YYYY-MM"} from _build_page_month_map.
        row_counter_start:         Counter for unique bank_id generation on new rows.

    Returns:
        (new_rows, stats)
    """
    import pdfplumber

    pdf_path = Path(pdf_path)

    # Index existing rows by check_no for O(1) lookup
    existing_by_check: dict[str, int] = {}
    for idx, row in enumerate(existing_rows):
        cn = str(row.get("check_no", "")).strip()
        if cn:
            existing_by_check.setdefault(cn, idx)

    doc = fitz.open(str(pdf_path))

    # Collect check-face pages using pdfplumber text + fitz image detection
    check_face_indices: list[int] = []
    with pdfplumber.open(pdf_path) as ppdf:
        for i, pp in enumerate(ppdf.pages):
            text = pp.extract_text() or ""
            if _is_check_face_page(doc, i, text):
                check_face_indices.append(i)

    logger.info(f"    LLM fallback: {len(check_face_indices)} check-face pages identified")

    new_rows:        list[dict] = []
    row_counter    = row_counter_start
    pages_processed = 0
    rows_extracted  = 0
    vendor_matches  = 0

    for page_0idx in check_face_indices:
        # Resolve statement month for this page
        page_1idx = page_0idx + 1
        page_sm   = statement_month_fallback
        for p in sorted(page_month_map.keys(), reverse=True):
            if p <= page_1idx:
                page_sm = page_month_map[p]
                break

        try:
            img_bytes = _page_image_bytes(doc, page_0idx)
            if img_bytes is None:
                logger.debug(f"    Page {page_0idx+1}: no usable image — skipping")
                continue
            extracted = llm_extract_check_fields(img_bytes)
        except Exception as e:
            logger.warning(f"    LLM fallback → page {page_0idx+1}: error — {e}")
            continue

        pages_processed += 1

        for item in extracted:
            rows_extracted += 1
            cn   = item["check_no"]
            desc = item["vendor"] or item["raw_text"] or ""

            logger.info(
                f"    LLM fallback → page {page_0idx+1} → "
                f"check_no={cn!r}  vendor={item['vendor']!r}"
            )

            if cn and cn in existing_by_check:
                # Enrich existing row with vendor name
                existing_idx = existing_by_check[cn]
                if not existing_rows[existing_idx].get("description", "").strip() and desc:
                    existing_rows[existing_idx]["description"] = desc
                    vendor_matches += 1
            else:
                # New check — add as a fresh row
                posted_date = None
                if item["check_date"]:
                    try:
                        posted_date = pd.to_datetime(item["check_date"])
                    except Exception:
                        pass
                if posted_date is None and len(page_sm) == 7:
                    try:
                        posted_date = pd.to_datetime(f"{page_sm}-01")
                    except Exception:
                        pass

                if item["amount"] is None:
                    continue   # Skip if no amount — useless for matching

                new_rows.append({
                    "bank_id":         f"{bank}_{page_sm}_LLM{row_counter:04d}",
                    "posted_date":     posted_date,
                    "check_no":        cn,
                    "amount":          item["amount"],
                    "description":     desc,
                    "statement_month": page_sm,
                    "source":          "llm_fallback",
                })
                row_counter += 1

    doc.close()

    stats = {
        "pages_processed": pages_processed,
        "rows_extracted":  rows_extracted,
        "vendor_matches":  vendor_matches,
        "new_records":     len(new_rows),
    }
    logger.info(
        f"    LLM fallback complete — "
        f"pages={pages_processed}, extracted={rows_extracted}, "
        f"vendor_enriched={vendor_matches}, new_records={len(new_rows)}"
    )
    return new_rows, stats
