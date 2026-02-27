from __future__ import annotations
"""
match/harmonize_records.py

Loads data/interim/pid.csv and data/interim/bank_<BANK>.csv.
Cleans, standardizes, and returns unified DataFrames ready for matching.

Harmonization applied:
  - Strip $ signs, commas from amounts → float
  - Standardize all dates to pandas Timestamp
  - Normalize check numbers (strip whitespace, remove .0 suffix)
  - Clean description text (collapse whitespace / control chars)
  - Normalize PID bank column to canonical identifier (FLAG / AG / WDCPL / GPM)
  - Add `amount_abs` to bank_df (used by matcher)
"""

import re
import sys
import logging
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DATA_INTERIM, BANKS

logger = logging.getLogger(__name__)

# Maps raw bank name strings → canonical ID
BANK_NAME_MAP: dict[str, str] = {
    "flag":             "FLAG",
    "ag":               "AG",
    "wdcpl":            "WDCPL",
    "gpm":              "GPM",
    # Common aliases — extend as needed
    "flagstar":         "FLAG",
    "american guaranty": "AG",
    "a&g":              "AG",
    "a & g":            "AG",
}


# ─── Field-level normalizers ──────────────────────────────────────────────────

def _norm_check_no(val) -> str:
    """Strip whitespace; remove trailing .0 from Excel float-cast integers."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip()
    if s.lower() in ("nan", "none"):
        return ""
    if s.endswith(".0"):
        s = s[:-2]
    return s.strip()


def _norm_amount(val) -> float | None:
    """Remove $, commas; handle parentheses as negative. Returns None if unparseable."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "-", ""):
        return None
    negative = s.startswith("(") and s.endswith(")")
    s = re.sub(r"[$,\(\)\s]", "", s)
    if not s:
        return None
    try:
        result = float(s)
        return -result if negative else result
    except ValueError:
        return None


def _norm_date(val) -> pd.Timestamp | None:
    """Parse date-like values to Timestamp; return None if unparseable."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, pd.Timestamp):
        return val
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", ""):
        return None
    try:
        return pd.to_datetime(s, infer_datetime_format=True)
    except Exception:
        return None


def _norm_bank_name(val: str) -> str:
    """Map raw bank name → canonical identifier."""
    if not val or str(val).strip().lower() in ("nan", "none", ""):
        return ""
    key = str(val).strip().lower()
    return BANK_NAME_MAP.get(key, val.strip().upper())


def _clean_text(val) -> str:
    """Collapse whitespace and control characters in description/vendor fields."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val)
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()


# ─── DataFrame-level harmonizers ──────────────────────────────────────────────

def harmonize_pid(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize PID DataFrame fields in-place (on a copy)."""
    df = df.copy()
    df["check_no"]     = df["check_no"].apply(_norm_check_no)
    df["amount"]       = df["amount"].apply(_norm_amount)
    df["invoice_date"] = df["invoice_date"].apply(_norm_date)
    df["check_date"]   = df["check_date"].apply(_norm_date)
    df["bank"]         = df["bank"].apply(_norm_bank_name)
    df["vendor"]       = df["vendor"].apply(_clean_text)
    df["description"]  = df.get("description", pd.Series("", index=df.index)).apply(_clean_text)
    df = df.dropna(subset=["amount"]).reset_index(drop=True)
    return df


def harmonize_bank(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize bank DataFrame fields in-place (on a copy)."""
    df = df.copy()
    df["check_no"]    = df["check_no"].apply(_norm_check_no)
    df["amount"]      = df["amount"].apply(_norm_amount)
    df["posted_date"] = df["posted_date"].apply(_norm_date)
    df["description"] = df["description"].apply(_clean_text)
    # Absolute value used by matcher (bank debits are stored negative)
    df["amount_abs"]  = df["amount"].abs()
    df = df.dropna(subset=["amount"]).reset_index(drop=True)
    return df


# ─── Loader ───────────────────────────────────────────────────────────────────

def load_and_harmonize(
    interim_dir: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load pid.csv and all bank_<BANK>.csv from data/interim/.
    Returns (pid_df, bank_df) — both harmonized and ready for matching.
    """
    if interim_dir is None:
        interim_dir = DATA_INTERIM

    # ── PID ──────────────────────────────────────────────────────────────────
    pid_path = interim_dir / "pid.csv"
    if not pid_path.exists():
        raise FileNotFoundError(f"PID CSV not found: {pid_path}")
    pid_df = pd.read_csv(pid_path)
    pid_df = harmonize_pid(pid_df)
    logger.info(f"PID loaded and harmonized: {len(pid_df)} records")

    # ── Banks ─────────────────────────────────────────────────────────────────
    bank_frames: list[pd.DataFrame] = []
    for bank in BANKS:
        bank_path = interim_dir / f"bank_{bank}.csv"
        if bank_path.exists():
            df = pd.read_csv(bank_path)
            df["bank_name"] = bank   # tag with canonical identifier
            bank_frames.append(df)
            logger.info(f"Bank {bank}: {len(df)} transactions loaded")
        else:
            logger.warning(f"No bank CSV found for {bank} at {bank_path}")

    if not bank_frames:
        raise FileNotFoundError(f"No bank CSVs found in {interim_dir}")

    bank_df = pd.concat(bank_frames, ignore_index=True)
    bank_df = harmonize_bank(bank_df)
    logger.info(f"Total bank transactions after harmonization: {len(bank_df)}")

    return pid_df, bank_df
