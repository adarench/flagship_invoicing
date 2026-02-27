from __future__ import annotations
"""
match/deterministic.py

Deterministic matching of PID records → bank transactions.

Matching tiers (applied in order; each tier operates only on still-unmatched rows):
  1. PRIMARY   — check_no exact AND amount exact
  2. SECONDARY — amount exact AND |posted_date − check_date| ≤ DATE_WINDOW_DAYS
  3. RETENTION — sum of 2 bank transactions matches PID amount (split payments)
  4. UNMATCHED — no match found

Output schema (one row per PID record):
    pid_id            str
    bank_id           str | None   (compound "id1+id2" for retention splits)
    match_type        str          primary | secondary | retention | unmatched
    match_confidence  float        0.0 – 1.0
    notes             str
"""

import sys
import logging
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DATE_WINDOW_DAYS, AMOUNT_TOLERANCE

logger = logging.getLogger(__name__)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _amounts_match(a: float, b: float, tol: float = AMOUNT_TOLERANCE) -> bool:
    """True if |abs(a) − abs(b)| ≤ tol."""
    return abs(abs(a) - abs(b)) <= tol


def _date_diff(d1, d2) -> int | None:
    """Absolute day difference between two date-like values. Returns None if either is NaT."""
    try:
        t1 = pd.to_datetime(d1)
        t2 = pd.to_datetime(d2)
        if pd.isna(t1) or pd.isna(t2):
            return None
        return abs((t1 - t2).days)
    except Exception:
        return None


# ─── Tier 1: Primary ─────────────────────────────────────────────────────────

def match_primary(
    pid_df: pd.DataFrame,
    bank_df: pd.DataFrame,
) -> tuple[list[dict], set, set]:
    """
    Primary match: check_no exact AND amount exact.
    One-to-one: a bank row can only match one PID row.
    Returns (matches, matched_pid_ids, matched_bank_ids).
    """
    matches: list[dict] = []
    matched_pid:  set[str] = set()
    matched_bank: set[str] = set()

    # Index bank rows with a check_no by that check_no for O(1) lookup
    bank_with_check = bank_df[bank_df["check_no"].str.strip() != ""].copy()
    bank_idx: dict[str, list] = {}
    for _, brow in bank_with_check.iterrows():
        bank_idx.setdefault(brow["check_no"].strip(), []).append(brow)

    for _, prow in pid_df.iterrows():
        pid_check = str(prow.get("check_no", "")).strip()
        if not pid_check:
            continue

        for brow in bank_idx.get(pid_check, []):
            if brow["bank_id"] in matched_bank:
                continue
            if _amounts_match(prow["amount"], brow["amount_abs"]):
                matches.append({
                    "pid_id":           prow["pid_id"],
                    "bank_id":          brow["bank_id"],
                    "match_type":       "primary",
                    "match_confidence": 1.0,
                    "notes":            f"check_no={pid_check}, amount={prow['amount']:.2f}",
                })
                matched_pid.add(prow["pid_id"])
                matched_bank.add(brow["bank_id"])
                break   # one-to-one

    logger.info(f"Primary matches: {len(matches)}")
    return matches, matched_pid, matched_bank


# ─── Tier 2: Secondary ───────────────────────────────────────────────────────

def match_secondary(
    pid_df: pd.DataFrame,
    bank_df: pd.DataFrame,
    matched_pid:  set,
    matched_bank: set,
) -> tuple[list[dict], set, set]:
    """
    Secondary match: amount exact AND date within DATE_WINDOW_DAYS.
    Operates only on rows not yet matched.
    """
    unmatched_pid  = pid_df[~pid_df["pid_id"].isin(matched_pid)].copy()
    unmatched_bank = bank_df[~bank_df["bank_id"].isin(matched_bank)].copy()

    matches:      list[dict] = []
    new_pid:      set[str]   = set()
    new_bank:     set[str]   = set()

    for _, prow in unmatched_pid.iterrows():
        pid_amount = prow.get("amount")
        check_date = prow.get("check_date")

        if pd.isna(pid_amount) or pd.isna(check_date):
            continue

        check_date_ts = pd.to_datetime(check_date)

        # Filter to amount-matching bank rows not yet claimed
        amount_ok = unmatched_bank[
            ~unmatched_bank["bank_id"].isin(new_bank) &
            unmatched_bank["amount_abs"].apply(lambda x: _amounts_match(pid_amount, x))
        ].copy()

        if amount_ok.empty:
            continue

        # Apply date window
        amount_ok["_diff"] = amount_ok["posted_date"].apply(
            lambda d: _date_diff(d, check_date_ts)
        )
        in_window = amount_ok[
            amount_ok["_diff"].notna() &
            (amount_ok["_diff"] <= DATE_WINDOW_DAYS)
        ]

        if in_window.empty:
            continue

        best = in_window.nsmallest(1, "_diff").iloc[0]
        diff = int(best["_diff"])
        # Confidence scales linearly from 1.0 (same day) to 0.8 (at window edge)
        confidence = round(1.0 - (diff / DATE_WINDOW_DAYS) * 0.2, 3)

        matches.append({
            "pid_id":           prow["pid_id"],
            "bank_id":          best["bank_id"],
            "match_type":       "secondary",
            "match_confidence": confidence,
            "notes":            f"amount={pid_amount:.2f}, date_diff={diff}d",
        })
        new_pid.add(prow["pid_id"])
        new_bank.add(best["bank_id"])

    logger.info(f"Secondary matches: {len(matches)}")
    return matches, matched_pid | new_pid, matched_bank | new_bank


# ─── Tier 3: Retention / Split ────────────────────────────────────────────────

def match_retention(
    pid_df: pd.DataFrame,
    bank_df: pd.DataFrame,
    matched_pid:  set,
    matched_bank: set,
) -> tuple[list[dict], set, set]:
    """
    Retention match: sum of two bank transactions = PID amount.
    Typical case: primary payment + retention release = full invoice amount.
    Only 2-way splits are handled in MVP 1.
    """
    unmatched_pid  = pid_df[~pid_df["pid_id"].isin(matched_pid)].copy()
    unmatched_bank = bank_df[~bank_df["bank_id"].isin(matched_bank)].copy()

    matches:  list[dict] = []
    new_pid:  set[str]   = set()
    new_bank: set[str]   = set()

    for _, prow in unmatched_pid.iterrows():
        pid_amount = prow.get("amount")
        if pd.isna(pid_amount):
            continue

        pid_amount = abs(pid_amount)
        check_date = prow.get("check_date")

        # Narrow bank pool to date window (if we have a check_date)
        pool = unmatched_bank[~unmatched_bank["bank_id"].isin(new_bank)].copy()
        if pd.notna(check_date):
            check_date_ts = pd.to_datetime(check_date)
            pool = pool[
                pool["posted_date"].notna() &
                pool["posted_date"].apply(
                    lambda d: (_date_diff(d, check_date_ts) or 9999) <= DATE_WINDOW_DAYS
                )
            ]

        if len(pool) < 2:
            continue

        # Check all pairs for sum match (O(n²) — pool is small)
        pool = pool.reset_index(drop=True)
        n = len(pool)
        found = False
        for i in range(n):
            if found:
                break
            for j in range(i + 1, n):
                total = abs(pool.iloc[i]["amount"]) + abs(pool.iloc[j]["amount"])
                if _amounts_match(pid_amount, total):
                    bid_i = pool.iloc[i]["bank_id"]
                    bid_j = pool.iloc[j]["bank_id"]
                    amt_i = abs(pool.iloc[i]["amount"])
                    amt_j = abs(pool.iloc[j]["amount"])
                    matches.append({
                        "pid_id":           prow["pid_id"],
                        "bank_id":          f"{bid_i}+{bid_j}",
                        "match_type":       "retention",
                        "match_confidence": 0.85,
                        "notes":            f"split: {amt_i:.2f}+{amt_j:.2f}={total:.2f}",
                    })
                    new_pid.add(prow["pid_id"])
                    new_bank.add(bid_i)
                    new_bank.add(bid_j)
                    found = True
                    break

    logger.info(f"Retention (split) matches: {len(matches)}")
    return matches, matched_pid | new_pid, matched_bank | new_bank


# ─── Tier 4: Tertiary (fuzzy vendor) ─────────────────────────────────────────

# Wider tolerances for fuzzy-matched entries
_FUZZY_AMOUNT_TOLERANCE_PCT = 0.05   # 5% of PID amount
_FUZZY_DATE_WINDOW          = 30     # days


def match_tertiary(
    pid_df:       pd.DataFrame,
    bank_df:      pd.DataFrame,
    matched_pid:  set,
    matched_bank: set,
) -> tuple[list[dict], set, set]:
    """
    Tertiary match: canonical_vendor_bank == pid.vendor
                    AND amount within 5%
                    AND date within 30 days.

    Requires bank_df to have a 'canonical_vendor' column populated by fuzzy_llm.
    Skips silently if the column is absent or all-empty.
    """
    if "canonical_vendor" not in bank_df.columns:
        logger.debug("Tertiary match skipped: no canonical_vendor column in bank_df")
        return [], matched_pid, matched_bank

    unmatched_pid  = pid_df[~pid_df["pid_id"].isin(matched_pid)].copy()
    unmatched_bank = bank_df[
        ~bank_df["bank_id"].isin(matched_bank) &
        bank_df["canonical_vendor"].fillna("").str.strip().ne("")
    ].copy()

    if unmatched_bank.empty:
        logger.info("Tertiary match: no bank rows with canonical_vendor — 0 matches")
        return [], matched_pid, matched_bank

    matches:  list[dict] = []
    new_pid:  set[str]   = set()
    new_bank: set[str]   = set()

    # Index bank by canonical_vendor
    bank_vendor_idx: dict[str, list] = {}
    for _, brow in unmatched_bank.iterrows():
        bank_vendor_idx.setdefault(brow["canonical_vendor"], []).append(brow)

    for _, prow in unmatched_pid.iterrows():
        pid_vendor = str(prow.get("vendor", "")).strip()
        if not pid_vendor:
            continue

        candidates = bank_vendor_idx.get(pid_vendor, [])
        if not candidates:
            continue

        pid_amount    = abs(float(prow.get("amount", 0) or 0))
        pid_date      = pd.to_datetime(prow.get("check_date")) if pd.notna(prow.get("check_date")) else None
        amount_tol    = pid_amount * _FUZZY_AMOUNT_TOLERANCE_PCT

        for brow in candidates:
            if brow["bank_id"] in new_bank:
                continue

            bank_amount = abs(float(brow.get("amount_abs") or brow.get("amount") or 0))
            if abs(pid_amount - bank_amount) > amount_tol:
                continue

            # Date check (optional — if either is missing, skip date filter)
            if pid_date is not None and pd.notna(brow.get("posted_date")):
                diff = _date_diff(brow["posted_date"], pid_date)
                if diff is None or diff > _FUZZY_DATE_WINDOW:
                    continue
                confidence = round(0.65 - (diff / _FUZZY_DATE_WINDOW) * 0.1, 3)
            else:
                confidence = 0.55

            matches.append({
                "pid_id":           prow["pid_id"],
                "bank_id":          brow["bank_id"],
                "match_type":       "fuzzy",
                "match_confidence": confidence,
                "notes":            f"vendor={pid_vendor!r}, amount_diff={abs(pid_amount - bank_amount):.2f}",
            })
            new_pid.add(prow["pid_id"])
            new_bank.add(brow["bank_id"])
            break

    logger.info(f"Tertiary (fuzzy vendor) matches: {len(matches)}")
    return matches, matched_pid | new_pid, matched_bank | new_bank


# ─── Orchestrator ─────────────────────────────────────────────────────────────

def run_matching(pid_df: pd.DataFrame, bank_df: pd.DataFrame) -> pd.DataFrame:
    """
    Run all matching tiers in order:
      1. Primary   — check_no exact + amount exact
      2. Secondary — amount exact + date window
      3. Retention — split payment sum
      4. Tertiary  — fuzzy vendor + amount proximity + date window
    Returns match DataFrame (one row per PID record).
    """
    all_matches: list[dict] = []

    primary_matches, matched_pid, matched_bank = match_primary(pid_df, bank_df)
    all_matches.extend(primary_matches)

    secondary_matches, matched_pid, matched_bank = match_secondary(
        pid_df, bank_df, matched_pid, matched_bank
    )
    all_matches.extend(secondary_matches)

    retention_matches, matched_pid, matched_bank = match_retention(
        pid_df, bank_df, matched_pid, matched_bank
    )
    all_matches.extend(retention_matches)

    tertiary_matches, matched_pid, matched_bank = match_tertiary(
        pid_df, bank_df, matched_pid, matched_bank
    )
    all_matches.extend(tertiary_matches)

    # Tag remaining PID rows as unmatched
    for _, prow in pid_df[~pid_df["pid_id"].isin(matched_pid)].iterrows():
        all_matches.append({
            "pid_id":           prow["pid_id"],
            "bank_id":          None,
            "match_type":       "unmatched",
            "match_confidence": 0.0,
            "notes":            "",
        })

    match_df = pd.DataFrame(all_matches)

    counts = match_df["match_type"].value_counts().to_dict()
    logger.info(
        f"Matching complete — "
        f"primary: {counts.get('primary', 0)}, "
        f"secondary: {counts.get('secondary', 0)}, "
        f"retention: {counts.get('retention', 0)}, "
        f"fuzzy: {counts.get('fuzzy', 0)}, "
        f"unmatched: {counts.get('unmatched', 0)}"
    )
    return match_df
