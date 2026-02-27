from __future__ import annotations
"""
output/generate_packet.py

MVP 2+ feature: Generate PDF support packets for each matched PID row.

Each packet (per pid_id) contains:
  Page 1: PID row details
  Page 2: Bank transaction snippet
  Page 3: LLM reasoning (short audit explanation via Claude Sonnet)

Output → output/packets/<pid_id>.pdf

STATUS: Stubbed for MVP 1. Full implementation in MVP 2.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def generate_packet(
    pid_row:    dict,
    bank_rows:  list[dict],
    reasoning:  str,
    output_dir: Path,
) -> Path:
    """
    Generate a PDF support packet for one matched PID record.

    Args:
        pid_row:    PID record as dict
        bank_rows:  List of matched bank transaction dicts
        reasoning:  LLM-generated audit explanation (from Claude Sonnet)
        output_dir: Directory to write PDF

    Returns:
        Path to generated PDF

    TODO (MVP 2):
        - Use reportlab (or fpdf2) to build a 3-page PDF
        - Page 1: formatted PID row details table
        - Page 2: bank statement snippet (transaction rows)
        - Page 3: LLM reasoning paragraph with confidence note
    """
    pid_id = pid_row.get("pid_id", "unknown")
    output_path = output_dir / f"{pid_id}.pdf"
    logger.warning(
        f"generate_packet: stub — packet generation not implemented yet for {pid_id}"
    )
    return output_path


def run(
    reconciled_df: pd.DataFrame,
    pid_df:        pd.DataFrame,
    bank_df:       pd.DataFrame,
    output_dir:    Path | None = None,
):
    """
    Generate packets for all matched records.
    TODO (MVP 2): implement full packet generation per matched PID row.
    """
    logger.info("Packet generation: stub — will be implemented in MVP 2")
