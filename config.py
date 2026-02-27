"""
config.py — Central configuration for Invoice Reconciliation Pipeline.
"""
from __future__ import annotations

from pathlib import Path
import os

from dotenv import load_dotenv
load_dotenv()

ROOT = Path(__file__).parent

# ── Data Directories ─────────────────────────────────────────────────────────
DATA_PID_RAW  = ROOT / "data" / "pid_raw"
DATA_BANK_RAW = ROOT / "data" / "bank_raw"
DATA_INTERIM  = ROOT / "data" / "interim"

# ── Output Directories ────────────────────────────────────────────────────────
OUTPUT_DIR  = ROOT / "output"
PACKETS_DIR = OUTPUT_DIR / "packets"
LOG_DIR     = ROOT / "logs"

# ── Canonical Bank Identifiers ────────────────────────────────────────────────
BANKS = ["FLAG", "AG", "WDCPL", "GPM"]

# ── Matching Parameters ───────────────────────────────────────────────────────
DATE_WINDOW_DAYS = 10       # Max days between check_date and posted_date
AMOUNT_TOLERANCE = 0.01     # Float tolerance for amount comparison (cents)

# ── Claude Models ─────────────────────────────────────────────────────────────
CLAUDE_SONNET = "claude-sonnet-4-6"            # PDF parsing, match reasoning
CLAUDE_HAIKU  = "claude-haiku-4-5-20251001"    # Vendor fuzzy matching

# ── API Key ───────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")


# ── Client Factories (MVP 2+) ─────────────────────────────────────────────────

def _make_client():
    """Return a configured Anthropic client. Raises if key is missing."""
    import anthropic
    if not ANTHROPIC_API_KEY:
        raise ValueError(
            "ANTHROPIC_API_KEY is not set. "
            "Add it to your .env file and re-run."
        )
    return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


def get_sonnet_client():
    """Anthropic client pre-configured for Claude Sonnet (PDF parsing / reasoning)."""
    return _make_client()


def get_haiku_client():
    """Anthropic client pre-configured for Claude Haiku (vendor fuzzy matching)."""
    return _make_client()
