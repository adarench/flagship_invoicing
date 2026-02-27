from __future__ import annotations
"""
output/packet_reasoning_llm.py

MVP 3: Generate LLM audit reasoning for reconciliation packets.

For each matched PID row, Claude 3.7 Sonnet produces a short audit explanation
that is included as Page 3 of the PDF support packet.

STATUS: Stub — LLM calls not yet implemented.
"""

import logging

logger = logging.getLogger(__name__)


def generate_reasoning(pid_row: dict, bank_rows: list[dict]) -> str:
    """
    Generate a concise audit explanation for a matched PID ↔ bank record.

    Args:
        pid_row:   Canonical PID record dict
        bank_rows: List of matched bank transaction dicts (1 for direct, 2 for split)

    Returns:
        Short paragraph (2–4 sentences) suitable for inclusion in the audit packet.

    TODO (MVP 3):
        - Build a prompt describing the match:
            "PID record: vendor={vendor}, check #{check_no}, amount=${amount}, date={check_date}.
             Matched to bank transaction(s): {bank_rows}.
             Match type: {match_type}. Confidence: {match_confidence}.
             Write a 2-3 sentence audit note explaining why this match is valid."
        - Model: config.CLAUDE_SONNET
        - Use config.get_sonnet_client()
        - Temperature: 0.2 (low, for consistent audit language)
        - Return the text content of the response
    """
    raise NotImplementedError(
        "generate_reasoning: LLM audit reasoning not yet implemented. "
        "Implement in MVP 3 using Claude 3.7 Sonnet."
    )


def batch_generate_reasoning(
    matched_records: list[dict],
    pid_df_idx: dict,
    bank_df_idx: dict,
) -> dict[str, str]:
    """
    Generate reasoning for all matched records.

    Args:
        matched_records: List of match dicts from deterministic.run_matching()
        pid_df_idx:      pid_id → pid_row dict
        bank_df_idx:     bank_id → bank_row dict

    Returns:
        Dict mapping pid_id → reasoning string

    TODO (MVP 3):
        - Filter to matched records only (skip unmatched)
        - Call generate_reasoning() for each
        - Handle rate limits with retry/backoff
        - Cache results to avoid re-generating on re-run
    """
    raise NotImplementedError(
        "batch_generate_reasoning: not yet implemented. "
        "Implement in MVP 3 using Claude 3.7 Sonnet."
    )
