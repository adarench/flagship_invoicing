from __future__ import annotations
"""
output/packet_reasoning_llm.py

Generate concise audit reasoning text for matched PID -> bank records.
Reasoning is cached on disk to avoid re-calling the model for identical inputs.
"""

import hashlib
import json
import logging
import time
from pathlib import Path

from config import CLAUDE_SONNET, OUTPUT_DIR, get_sonnet_client

logger = logging.getLogger(__name__)

_DEFAULT_CACHE_PATH = OUTPUT_DIR / "packet_reasoning_cache.json"


def _load_cache(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        logger.warning(f"Failed to load reasoning cache {path}: {exc}")
        return {}


def _save_cache(path: Path, cache: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, indent=2, sort_keys=True))


def _fallback_reasoning(
    pid_row: dict,
    bank_rows: list[dict],
    match_type: str | None,
    match_confidence: float | None,
) -> str:
    vendor = str(pid_row.get("vendor", "") or "Unknown vendor")
    check_no = str(pid_row.get("check_no", "") or "N/A")
    amount = float(abs(pid_row.get("amount", 0) or 0))
    confidence_pct = f"{round((match_confidence or 0.0) * 100):.0f}%"
    bank_count = len(bank_rows)
    return (
        f"PID payment for {vendor} (check {check_no}, ${amount:,.2f}) aligns to "
        f"{bank_count} bank transaction(s) under the {match_type or 'unknown'} match tier. "
        f"The match confidence is {confidence_pct}, based on amount/date/check consistency. "
        "Manual review can confirm supporting statement details and memo text."
    )


def _build_prompt(
    pid_row: dict,
    bank_rows: list[dict],
    match_type: str | None,
    match_confidence: float | None,
    notes: str | None,
) -> str:
    return (
        "Write a concise 2-4 sentence audit rationale for this reconciliation match.\n"
        "Keep the tone factual and compliance-oriented. Mention concrete evidence.\n"
        "Do not invent fields.\n\n"
        f"PID record:\n{json.dumps(pid_row, default=str, indent=2)}\n\n"
        f"Bank record(s):\n{json.dumps(bank_rows, default=str, indent=2)}\n\n"
        f"Match type: {match_type or ''}\n"
        f"Match confidence: {match_confidence if match_confidence is not None else ''}\n"
        f"Matcher notes: {notes or ''}\n\n"
        "Return plain text only."
    )


def _cache_key(
    pid_row: dict,
    bank_rows: list[dict],
    match_type: str | None,
    match_confidence: float | None,
    notes: str | None,
) -> str:
    payload = {
        "pid_row": pid_row,
        "bank_rows": bank_rows,
        "match_type": match_type,
        "match_confidence": match_confidence,
        "notes": notes,
    }
    raw = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def generate_reasoning(
    pid_row: dict,
    bank_rows: list[dict],
    match_type: str | None = None,
    match_confidence: float | None = None,
    notes: str | None = None,
    cache_path: Path | None = None,
    max_retries: int = 2,
) -> str:
    """
    Generate concise audit reasoning for one match.
    Uses local cache and falls back to deterministic text if LLM is unavailable.
    """
    cache_path = cache_path or _DEFAULT_CACHE_PATH
    key = _cache_key(pid_row, bank_rows, match_type, match_confidence, notes)
    cache = _load_cache(cache_path)
    if key in cache:
        return cache[key]

    prompt = _build_prompt(pid_row, bank_rows, match_type, match_confidence, notes)

    text: str | None = None
    try:
        client = get_sonnet_client()
        for attempt in range(max_retries + 1):
            try:
                resp = client.messages.create(
                    model=CLAUDE_SONNET,
                    max_tokens=220,
                    temperature=0.2,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = resp.content[0].text.strip()
                break
            except Exception as exc:
                if attempt >= max_retries:
                    logger.warning(f"Reasoning call failed after retries: {exc}")
                    break
                time.sleep(1.5 * (attempt + 1))
    except Exception as exc:
        logger.warning(f"Reasoning LLM unavailable, using fallback: {exc}")

    if not text:
        text = _fallback_reasoning(pid_row, bank_rows, match_type, match_confidence)

    cache[key] = text
    _save_cache(cache_path, cache)
    return text


def batch_generate_reasoning(
    matched_records: list[dict],
    pid_df_idx: dict,
    bank_df_idx: dict,
    cache_path: Path | None = None,
) -> dict[str, str]:
    """
    Generate reasoning for all matched records.
    Returns mapping of match key (`pid_id:bank_id`) to reasoning text.
    """
    cache_path = cache_path or _DEFAULT_CACHE_PATH
    out: dict[str, str] = {}

    for record in matched_records:
        if record.get("match_type") == "unmatched":
            continue
        pid_id = record.get("pid_id")
        bank_id = record.get("bank_id")
        if not pid_id or pid_id not in pid_df_idx:
            continue

        pid_row = pid_df_idx[pid_id]

        bank_rows: list[dict] = []
        for bid in str(bank_id or "").split("+"):
            if bid and bid in bank_df_idx:
                bank_rows.append(bank_df_idx[bid])

        if not bank_rows:
            # Keep behavior deterministic even when bank rows are unavailable.
            bank_rows = [{"bank_id": str(bank_id or "")}]

        reasoning = generate_reasoning(
            pid_row=pid_row,
            bank_rows=bank_rows,
            match_type=record.get("match_type"),
            match_confidence=float(record.get("match_confidence", 0) or 0),
            notes=str(record.get("notes", "")),
            cache_path=cache_path,
        )
        out[f"{pid_id}:{bank_id or 'none'}"] = reasoning

    return out
