from __future__ import annotations
"""
match/fuzzy_llm.py

MVP 2: LLM-powered vendor name canonicalization using Claude 3.5 Haiku.

Maps raw bank description strings → canonical PID vendor names.
Used to power the tertiary matching tier in deterministic.py.

Cache stored at: data/interim/vendor_canonical_map.json
Re-running is idempotent — only uncached descriptions are sent to the LLM.

Usage:
    from match.fuzzy_llm import batch_canonicalize_vendor_names
    mapping = batch_canonicalize_vendor_names(bank_descriptions, pid_vendors)
    # mapping["J LYNE ROBERTS"] == "J.LYNE ROBERTS & SONS, INC"
    # mapping["Debit Transfer FUND TRF#..."] == "UNKNOWN"
"""

import json
import logging
import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import get_haiku_client, CLAUDE_HAIKU, DATA_INTERIM

logger = logging.getLogger(__name__)

CACHE_PATH = DATA_INTERIM / "vendor_canonical_map.json"
BATCH_SIZE  = 20   # descriptions per LLM call
MAX_VENDORS = 60   # max vendor candidates sent per prompt


# ─── Cache helpers ────────────────────────────────────────────────────────────

def _load_cache() -> dict[str, str]:
    if CACHE_PATH.exists():
        try:
            with open(CACHE_PATH) as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load vendor cache: {e}")
    return {}


def _save_cache(cache: dict[str, str]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=2, sort_keys=True)
    logger.debug(f"Vendor cache saved: {len(cache)} entries → {CACHE_PATH}")


# ─── Prompt builder ───────────────────────────────────────────────────────────

def _build_batch_prompt(descriptions: list[str], pid_vendors: list[str]) -> str:
    vendor_list = "\n".join(f"- {v}" for v in sorted(set(pid_vendors))[:MAX_VENDORS])
    numbered    = "\n".join(f'{i+1}. "{d}"' for i, d in enumerate(descriptions))
    return f"""\
You are reconciling bank statement descriptions to vendor names for a construction payment audit.

Known vendor names (exact spelling — use these exactly):
{vendor_list}

Bank descriptions to match ({len(descriptions)} items):
{numbered}

Rules:
- Reply with ONLY a JSON array of {len(descriptions)} strings.
- Each element must be the EXACT vendor name from the list above.
- Use "UNKNOWN" if no vendor clearly matches (e.g. internal transfers, unrecognised text).
- Do not explain. No markdown. Just the JSON array.

Example reply: ["J.LYNE ROBERTS & SONS, INC", "UNKNOWN", "LANDMARK EXCAVATING INC"]"""


# ─── LLM call ─────────────────────────────────────────────────────────────────

def _call_haiku(descriptions: list[str], pid_vendors: list[str]) -> list[str]:
    """Send one batch to Claude Haiku. Returns list of canonical names (same length)."""
    client = get_haiku_client()
    prompt = _build_batch_prompt(descriptions, pid_vendors)

    response = client.messages.create(
        model=CLAUDE_HAIKU,
        max_tokens=600,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = response.content[0].text.strip()

    # Strip markdown code fences if present
    raw = re.sub(r"^```[a-z]*\n?", "", raw).rstrip("`").strip()

    results: list[str] = json.loads(raw)

    if len(results) != len(descriptions):
        logger.warning(
            f"LLM returned {len(results)} results for batch of {len(descriptions)} — "
            "padding with UNKNOWN"
        )
        results.extend(["UNKNOWN"] * (len(descriptions) - len(results)))

    return results[:len(descriptions)]


# ─── Public API ───────────────────────────────────────────────────────────────

def canonicalize_vendor_name(
    text: str,
    pid_vendors: list[str],
    cache: dict[str, str] | None = None,
) -> str:
    """
    Canonicalize a single bank description string.
    Returns the matched PID vendor name or "UNKNOWN".
    Uses cache if provided; makes one LLM call if not cached.
    """
    if not text or text.strip().lower() in ("", "nan", "none"):
        return ""

    text = text.strip()
    if cache and text in cache:
        return cache[text]

    results = _call_haiku([text], pid_vendors)
    result  = results[0] if results else "UNKNOWN"

    # Validate against known vendors
    if result not in pid_vendors:
        result = "UNKNOWN"

    return result


def batch_canonicalize_vendor_names(
    descriptions: list[str],
    pid_vendors:  list[str],
) -> dict[str, str]:
    """
    Canonicalize a list of raw bank description strings.

    Args:
        descriptions:  List of bank description strings (may contain duplicates / empty)
        pid_vendors:   List of canonical vendor names from PID

    Returns:
        Dict mapping each unique non-empty description → canonical vendor name or "UNKNOWN"

    Writes results to data/interim/vendor_canonical_map.json (idempotent cache).
    """
    # Deduplicate and filter empties
    unique = list({
        d.strip() for d in descriptions
        if d and str(d).strip().lower() not in ("", "nan", "none")
    })

    if not unique:
        logger.info("No non-empty bank descriptions to canonicalize")
        return {}

    cache = _load_cache()
    to_process = [d for d in unique if d not in cache]

    if not to_process:
        logger.info(
            f"All {len(unique)} descriptions already in cache — "
            "skipping LLM calls"
        )
    else:
        logger.info(
            f"Canonicalizing {len(to_process)} descriptions via Claude Haiku "
            f"(cache hits: {len(unique) - len(to_process)})"
        )

        vendor_set = sorted(set(pid_vendors))
        n_batches  = (len(to_process) + BATCH_SIZE - 1) // BATCH_SIZE

        for batch_idx in range(n_batches):
            batch = to_process[batch_idx * BATCH_SIZE : (batch_idx + 1) * BATCH_SIZE]
            logger.info(f"  Batch {batch_idx + 1}/{n_batches}: {len(batch)} descriptions")
            try:
                results = _call_haiku(batch, vendor_set)
                for desc, result in zip(batch, results):
                    matched = result if result in vendor_set else "UNKNOWN"
                    cache[desc] = matched
                    if matched != "UNKNOWN":
                        logger.info(f"    Matched: {desc!r} → {matched!r}")
                    else:
                        logger.debug(f"    No match: {desc!r}")
            except Exception as e:
                logger.warning(f"  Batch {batch_idx + 1} failed: {e}")
                for desc in batch:
                    cache[desc] = "UNKNOWN"

        _save_cache(cache)

    # Build result dict for all original descriptions
    result_map: dict[str, str] = {}
    for d in descriptions:
        if d and str(d).strip().lower() not in ("", "nan", "none"):
            result_map[d.strip()] = cache.get(d.strip(), "UNKNOWN")

    matched_count = sum(1 for v in result_map.values() if v != "UNKNOWN")
    logger.info(
        f"Vendor canonicalization complete: "
        f"{matched_count}/{len(result_map)} descriptions matched to PID vendors"
    )
    return result_map


def add_canonical_vendor_column(
    bank_df:    pd.DataFrame,
    pid_df:     pd.DataFrame,
) -> pd.DataFrame:
    """
    Adds a 'canonical_vendor' column to bank_df by canonicalizing description strings.
    Rows with empty description or no match get canonical_vendor = "".
    """
    pid_vendors = pid_df["vendor"].dropna().unique().tolist()

    # Get non-empty unique descriptions
    descs = bank_df["description"].fillna("").tolist()
    non_empty_descs = [d for d in descs if d.strip()]

    if not non_empty_descs:
        logger.info("All bank descriptions are empty — canonical_vendor column will be empty")
        bank_df = bank_df.copy()
        bank_df["canonical_vendor"] = ""
        return bank_df

    mapping = batch_canonicalize_vendor_names(non_empty_descs, pid_vendors)

    bank_df = bank_df.copy()
    bank_df["canonical_vendor"] = bank_df["description"].apply(
        lambda d: mapping.get(str(d).strip(), "") if d and str(d).strip() else ""
    )
    # Replace "UNKNOWN" with empty string (cleaner for matching logic)
    bank_df["canonical_vendor"] = bank_df["canonical_vendor"].replace("UNKNOWN", "")

    return bank_df
