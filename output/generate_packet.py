from __future__ import annotations
"""
output/generate_packet.py

Generate PDF evidence packets for reconciliation matches.
Each packet contains:
  Page 1: PID details and match metadata
  Page 2: Matched bank transaction evidence
  Page 3: Audit reasoning narrative
"""

import logging
from pathlib import Path

import pandas as pd

from config import PACKETS_DIR

logger = logging.getLogger(__name__)


def _safe_packet_name(value: str) -> str:
    keep = []
    for ch in value:
        if ch.isalnum() or ch in ("-", "_", "."):
            keep.append(ch)
        else:
            keep.append("_")
    return "".join(keep).strip("_") or "packet"


def _build_table(rows: list[list[str]], col_widths: list[float]):
    from reportlab.lib import colors
    from reportlab.platypus import Table, TableStyle

    table = Table(rows, colWidths=col_widths)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4b99")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f6f8fc")]),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#c5cbd8")),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    return table


def generate_packet(
    pid_row: dict,
    bank_rows: list[dict],
    reasoning: str,
    output_dir: Path,
    match_meta: dict | None = None,
    packet_basename: str | None = None,
) -> Path:
    """
    Generate a 3-page PDF support packet for one matched PID record.
    """
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.units import inch
        from reportlab.platypus import (
            PageBreak,
            Paragraph,
            SimpleDocTemplate,
            Spacer,
        )
    except ImportError as exc:
        raise RuntimeError("reportlab is required to generate packets") from exc

    match_meta = match_meta or {}
    output_dir.mkdir(parents=True, exist_ok=True)
    pid_id = str(pid_row.get("pid_id", "unknown"))
    packet_seed = packet_basename or f"packet_{pid_id}"
    packet_name = _safe_packet_name(f"{packet_seed}.pdf")
    output_path = output_dir / packet_name

    doc = SimpleDocTemplate(str(output_path), pagesize=letter)
    styles = getSampleStyleSheet()
    story = []

    # Page 1: PID details + match metadata
    story.append(Paragraph("Reconciliation Evidence Packet", styles["Title"]))
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph("Page 1 of 3 - PID Record & Match Metadata", styles["Heading3"]))
    story.append(Spacer(1, 0.1 * inch))
    pid_rows = [
        ["Field", "Value"],
        ["PID ID", str(pid_row.get("pid_id", ""))],
        ["Vendor", str(pid_row.get("vendor", ""))],
        ["Invoice #", str(pid_row.get("invoice_no", ""))],
        ["Invoice Date", str(pid_row.get("invoice_date", ""))],
        ["PID Amount", f"${float(abs(pid_row.get('amount', 0) or 0)):.2f}"],
        ["Check #", str(pid_row.get("check_no", ""))],
        ["Check Date", str(pid_row.get("check_date", ""))],
        ["Bank", str(pid_row.get("bank", ""))],
        ["Phase", str(pid_row.get("phase", ""))],
        ["Reference", str(pid_row.get("reference", ""))],
        ["Match Type", str(match_meta.get("match_type", ""))],
        ["Match Confidence", f"{float(match_meta.get('match_confidence', 0) or 0) * 100:.1f}%"],
        ["Matcher Notes", str(match_meta.get("notes", ""))],
    ]
    story.append(_build_table(pid_rows, [2.0 * inch, 4.2 * inch]))

    # Page 2: bank evidence
    story.append(PageBreak())
    story.append(Paragraph("Reconciliation Evidence Packet", styles["Title"]))
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph("Page 2 of 3 - Bank Transaction Evidence", styles["Heading3"]))
    story.append(Spacer(1, 0.1 * inch))
    bank_table = [["Bank ID", "Posted Date", "Check #", "Amount", "Description"]]
    for row in bank_rows:
        bank_table.append(
            [
                str(row.get("bank_id", "")),
                str(row.get("posted_date", "")),
                str(row.get("check_no", "")),
                f"${float(abs(row.get('amount', 0) or 0)):.2f}",
                str(row.get("description", ""))[:120],
            ]
        )
    if len(bank_table) == 1:
        bank_table.append(["-", "-", "-", "-", "No linked bank rows found"])
    story.append(_build_table(bank_table, [1.35 * inch, 1.1 * inch, 0.8 * inch, 0.9 * inch, 2.05 * inch]))

    # Page 3: reasoning narrative
    story.append(PageBreak())
    story.append(Paragraph("Reconciliation Evidence Packet", styles["Title"]))
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph("Page 3 of 3 - Audit Reasoning", styles["Heading3"]))
    story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph(reasoning or "No reasoning available.", styles["BodyText"]))

    doc.build(story)
    logger.info(f"Generated packet PDF: {output_path}")
    return output_path


def run(
    reconciled_df: pd.DataFrame,
    pid_df: pd.DataFrame,
    bank_df: pd.DataFrame,
    output_dir: Path | None = None,
    reasoning_by_match: dict[str, str] | None = None,
) -> list[Path]:
    """
    Generate packets for all matched records in reconciled_df.
    """
    output_dir = output_dir or PACKETS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    reasoning_by_match = reasoning_by_match or {}

    pid_idx = {
        str(row["pid_id"]): row.to_dict()
        for _, row in pid_df.iterrows()
    }
    bank_idx = {
        str(row["bank_id"]): row.to_dict()
        for _, row in bank_df.iterrows()
    }

    generated: list[Path] = []
    for _, row in reconciled_df.iterrows():
        pid_id = str(row.get("pid_id", ""))
        bank_id = str(row.get("bank_id", ""))
        if not pid_id:
            continue

        pid_row = pid_idx.get(pid_id, {"pid_id": pid_id})
        bank_rows: list[dict] = []
        for bid in bank_id.split("+"):
            if bid in bank_idx:
                bank_rows.append(bank_idx[bid])

        match_key = f"{pid_id}:{bank_id or 'none'}"
        reasoning = reasoning_by_match.get(match_key, "")
        path = generate_packet(
            pid_row=pid_row,
            bank_rows=bank_rows,
            reasoning=reasoning,
            output_dir=output_dir,
            match_meta=row.to_dict(),
        )
        generated.append(path)
    return generated
