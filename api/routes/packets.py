from __future__ import annotations
"""
api/routes/packets.py

POST /api/jobs/{job_id}/match/{match_id}/packet  — generate a PDF packet
GET  /api/jobs/{job_id}/packets.zip              — download all packets as zip
"""

import io
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from api import storage
from api.models import PacketGenerateResponse

router = APIRouter()


def _generate_pdf(job_id: str, match_id: str, item: dict) -> Path:
    """Create a simple PDF audit packet using reportlab."""
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.units import inch
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib import colors
    except ImportError:
        raise HTTPException(status_code=500, detail="reportlab not installed")

    pdir = storage.packets_dir(job_id)
    safe_id = match_id.replace(":", "_").replace("+", "_")
    pdf_path = pdir / f"packet_{safe_id}.pdf"

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter)
    styles = getSampleStyleSheet()
    story = []

    # Title
    story.append(Paragraph("Reconciliation Audit Packet", styles["Title"]))
    story.append(Spacer(1, 0.2 * inch))

    # Meta
    story.append(Paragraph(f"Job ID: {job_id}", styles["Normal"]))
    story.append(Paragraph(f"Match ID: {match_id}", styles["Normal"]))
    story.append(Paragraph(
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        styles["Normal"],
    ))
    story.append(Spacer(1, 0.3 * inch))

    # Match data table
    rows = [
        ["Field", "Value"],
        ["Vendor", item.get("vendor", "")],
        ["Invoice #", item.get("invoice_no", "")],
        ["PID Amount", f"${float(item.get('pid_amount', 0)):.2f}"],
        ["Bank Amount", f"${float(item.get('bank_amount', 0) or 0):.2f}"],
        ["Match Type", item.get("match_type", "")],
        ["Confidence", f"{float(item.get('match_confidence', 0)) * 100:.0f}%"],
        ["Check #", item.get("check_no", "")],
        ["Check Date", str(item.get("check_date", ""))],
        ["Bank Posted", str(item.get("bank_posted_date", ""))],
        ["Bank", item.get("bank", "")],
        ["Notes", item.get("notes", "")],
        ["Review Status", item.get("status", "")],
    ]
    t = Table(rows, colWidths=[2 * inch, 4 * inch])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2D6CDF")),
        ("TEXTCOLOR",  (0, 0), (-1, 0), colors.white),
        ("FONTNAME",   (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f5f5f5")]),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    story.append(t)

    doc.build(story)
    pdf_path.write_bytes(buf.getvalue())
    return pdf_path


@router.post("/{job_id}/match/{match_id}/packet", response_model=PacketGenerateResponse)
async def generate_packet(job_id: str, match_id: str):
    # Load review item
    try:
        data = storage.read_json(job_id, "review_queue.json")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Review queue not found")

    item = next((i for i in data.get("items", []) if i["match_id"] == match_id), None)
    if not item:
        raise HTTPException(status_code=404, detail=f"Match {match_id!r} not found")

    pdf_path = _generate_pdf(job_id, match_id, item)
    packet_url = f"/api/jobs/{job_id}/exports/packets/{pdf_path.name}"

    return PacketGenerateResponse(match_id=match_id, packet_url=packet_url)


@router.get("/{job_id}/packets.zip")
async def download_packets_zip(job_id: str):
    pdir = storage.packets_dir(job_id)
    pdfs = list(pdir.glob("*.pdf"))
    if not pdfs:
        raise HTTPException(status_code=404, detail="No packets generated yet for this job")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for pdf in pdfs:
            zf.write(pdf, pdf.name)
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=packets_{job_id[:8]}.zip"},
    )
