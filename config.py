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


# ── Runtime Tuning (Step 2.1 performance) ───────────────────────────────────

def _env_int(name: str, default: int, min_value: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        val = int(raw)
        return max(min_value, val)
    except ValueError:
        return default


def _env_float(name: str, default: float, min_value: float = 0.0) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        val = float(raw)
        return max(min_value, val)
    except ValueError:
        return default


_CPU_COUNT = os.cpu_count() or 2
# Worker pool for per-PDF parsing.
BANK_PARSE_WORKERS = _env_int("BANK_PARSE_WORKERS", min(4, _CPU_COUNT), min_value=1)
# In-flight page OCR calls per PDF during LLM fallback.
LLM_PAGE_WORKERS = _env_int("LLM_PAGE_WORKERS", 4, min_value=1)
# Retry count for transient LLM page OCR failures.
LLM_PAGE_MAX_RETRIES = _env_int("LLM_PAGE_MAX_RETRIES", 2, min_value=0)
# Base backoff seconds between retries (scaled linearly by attempt).
LLM_PAGE_RETRY_BASE_SECONDS = _env_float("LLM_PAGE_RETRY_BASE_SECONDS", 1.0, min_value=0.0)


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
