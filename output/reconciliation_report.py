from __future__ import annotations
"""
output/reconciliation_report.py

Generates:
  output/reconciled.xlsx  — all matched PID rows with bank details
  output/unmatched.xlsx   — PID rows with no bank match

Inputs:
  pid_df   (harmonized PID DataFrame)
  bank_df  (harmonized bank DataFrame)
  match_df (output of deterministic.run_matching)
"""

import sys
import logging
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import OUTPUT_DIR

logger = logging.getLogger(__name__)


# ─── Build Output Frames ──────────────────────────────────────────────────────

def _build_frames(
    pid_df: pd.DataFrame,
    bank_df: pd.DataFrame,
    match_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Join match results with PID and bank data.
    Returns (reconciled_df, unmatched_df).
    """
    pid_idx  = pid_df.set_index("pid_id")
    bank_idx = bank_df.set_index("bank_id")

    reconciled_rows: list[dict] = []
    unmatched_rows:  list[dict] = []

    for _, mrow in match_df.iterrows():
        pid_id = mrow["pid_id"]
        if pid_id not in pid_idx.index:
            logger.warning(f"match references unknown pid_id: {pid_id}")
            continue

        prow = pid_idx.loc[pid_id]
        base = {
            "pid_id":           pid_id,
            "vendor":           prow.get("vendor", ""),
            "invoice_no":       prow.get("invoice_no", ""),
            "invoice_date":     prow.get("invoice_date", ""),
            "pid_amount":       prow.get("amount", ""),
            "check_no":         prow.get("check_no", ""),
            "check_date":       prow.get("check_date", ""),
            "bank":             prow.get("bank", ""),
            "phase":            prow.get("phase", ""),
            "reference":        prow.get("reference", ""),
            "match_type":       mrow["match_type"],
            "match_confidence": mrow["match_confidence"],
            "notes":            mrow.get("notes", ""),
        }

        if mrow["match_type"] == "unmatched" or not mrow["bank_id"]:
            unmatched_rows.append(base)
            continue

        # Retention splits have compound bank_id: "id1+id2"
        bank_ids    = str(mrow["bank_id"]).split("+")
        bank_amounts: list[float]  = []
        bank_dates:   list[str]   = []
        bank_descs:   list[str]   = []

        for bid in bank_ids:
            if bid in bank_idx.index:
                brow = bank_idx.loc[bid]
                bank_amounts.append(float(brow.get("amount", 0)))
                bank_dates.append(str(brow.get("posted_date", "")))
                bank_descs.append(str(brow.get("description", "")))
            else:
                logger.warning(f"match references unknown bank_id: {bid}")

        pid_amount = float(prow.get("amount", 0))
        bank_total = sum(abs(a) for a in bank_amounts)

        reconciled_rows.append({
            **base,
            "bank_id":          mrow["bank_id"],
            "bank_posted_date": " | ".join(bank_dates),
            "bank_amount":      bank_total,
            "bank_description": " | ".join(bank_descs),
            "amount_diff":      round(pid_amount - bank_total, 2),
        })

    reconciled_df = pd.DataFrame(reconciled_rows) if reconciled_rows else pd.DataFrame()
    unmatched_df  = pd.DataFrame(unmatched_rows)  if unmatched_rows  else pd.DataFrame()

    return reconciled_df, unmatched_df


# ─── Excel Writer ─────────────────────────────────────────────────────────────

def _write_excel(df: pd.DataFrame, path: Path, sheet_name: str):
    """Write DataFrame to Excel with auto-width columns."""
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        try:
            worksheet = writer.sheets[sheet_name]
            for i, col in enumerate(df.columns):
                max_len = max(
                    df[col].astype(str).apply(len).max(),
                    len(str(col))
                ) + 2
                worksheet.set_column(i, i, min(max_len, 55))
        except Exception as e:
            logger.debug(f"Could not auto-size columns: {e}")


# ─── Public Entry Point ───────────────────────────────────────────────────────

def generate_report(
    pid_df:     pd.DataFrame,
    bank_df:    pd.DataFrame,
    match_df:   pd.DataFrame,
    output_dir: Path | None = None,
) -> tuple[Path, Path]:
    """
    Generate reconciled.xlsx and unmatched.xlsx.
    Returns (reconciled_path, unmatched_path).
    """
    if output_dir is None:
        output_dir = OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    reconciled_df, unmatched_df = _build_frames(pid_df, bank_df, match_df)

    reconciled_path = output_dir / "reconciled.xlsx"
    unmatched_path  = output_dir / "unmatched.xlsx"

    if not reconciled_df.empty:
        _write_excel(reconciled_df, reconciled_path, "Reconciled")
        logger.info(f"Saved reconciled.xlsx ({len(reconciled_df)} rows) → {reconciled_path}")
    else:
        logger.warning("No reconciled records to write")

    if not unmatched_df.empty:
        _write_excel(unmatched_df, unmatched_path, "Unmatched")
        logger.info(f"Saved unmatched.xlsx ({len(unmatched_df)} rows) → {unmatched_path}")
    else:
        logger.info("No unmatched records")

    return reconciled_path, unmatched_path
