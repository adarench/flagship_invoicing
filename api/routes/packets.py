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

import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from api import storage
from api.models import PacketGenerateResponse
from output.generate_packet import generate_packet as generate_packet_pdf
from output.packet_reasoning_llm import generate_reasoning

router = APIRouter()


def _match_item(job_id: str, match_id: str) -> dict:
    try:
        queue = storage.read_json(job_id, "review_queue.json")
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Review queue not found") from exc
    item = next((i for i in queue.get("items", []) if i["match_id"] == match_id), None)
    if not item:
        raise HTTPException(status_code=404, detail=f"Match {match_id!r} not found")
    return item


def _load_pid_row(job_id: str, pid_id: str) -> dict:
    pid_path = storage.artifact_path(job_id, "pid.csv")
    if not pid_path.exists():
        raise HTTPException(status_code=404, detail="PID artifact not found for this job")

    pid_df = pd.read_csv(pid_path, dtype=str).fillna("")
    row_df = pid_df[pid_df["pid_id"] == pid_id]
    if row_df.empty:
        raise HTTPException(status_code=404, detail=f"PID row {pid_id!r} not found")
    return row_df.iloc[0].to_dict()


def _load_bank_rows(job_id: str, bank_id: str | None) -> list[dict]:
    if not bank_id:
        return []

    bank_ids = [s for s in str(bank_id).split("+") if s]
    if not bank_ids:
        return []

    bank_files = sorted(storage.job_dir(job_id).glob("bank_*.csv"))
    if not bank_files:
        return []

    frames = [pd.read_csv(path).fillna("") for path in bank_files]
    bank_df = pd.concat(frames, ignore_index=True)
    idx = bank_df.set_index("bank_id")
    out = []
    for bid in bank_ids:
        if bid in idx.index:
            row = idx.loc[bid]
            # If duplicate bank_id exists, use first row deterministically.
            if hasattr(row, "to_dict"):
                if isinstance(row, pd.DataFrame):
                    out.append(row.iloc[0].to_dict())
                else:
                    out.append(row.to_dict())
    return out


def _manifest_path(job_id: str) -> Path:
    return storage.artifact_path(job_id, "packet_manifest.json")


def _read_manifest(job_id: str) -> dict:
    path = _manifest_path(job_id)
    if path.exists():
        return storage.read_json(job_id, "packet_manifest.json")
    return {"job_id": job_id, "generated_at": None, "packets": []}


def _write_manifest(job_id: str, manifest: dict) -> None:
    manifest["generated_at"] = datetime.now(timezone.utc).isoformat()
    storage.write_json(job_id, "packet_manifest.json", manifest)


@router.post("/{job_id}/match/{match_id}/packet", response_model=PacketGenerateResponse)
async def generate_packet(job_id: str, match_id: str):
    item = _match_item(job_id, match_id)

    pid_id = str(item.get("pid_id", ""))
    pid_row = _load_pid_row(job_id, pid_id)
    bank_rows = _load_bank_rows(job_id, item.get("bank_id"))

    reasoning = generate_reasoning(
        pid_row=pid_row,
        bank_rows=bank_rows,
        match_type=str(item.get("match_type", "")),
        match_confidence=float(item.get("match_confidence", 0) or 0),
        notes=str(item.get("notes", "")),
        cache_path=storage.artifact_path(job_id, "packet_reasoning_cache.json"),
    )

    packet_seed = f"packet_{match_id.replace(':', '_').replace('+', '_')}"
    pdf_path = generate_packet_pdf(
        pid_row=pid_row,
        bank_rows=bank_rows,
        reasoning=reasoning,
        output_dir=storage.packets_dir(job_id),
        match_meta=item,
        packet_basename=packet_seed,
    )

    packet_url = f"/api/jobs/{job_id}/exports/packets/{pdf_path.name}"

    manifest = _read_manifest(job_id)
    packets = [p for p in manifest.get("packets", []) if p.get("match_id") != match_id]
    packets.append(
        {
            "match_id": match_id,
            "filename": pdf_path.name,
            "url": packet_url,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "status": "generated",
        }
    )
    manifest["packets"] = packets
    _write_manifest(job_id, manifest)

    return PacketGenerateResponse(match_id=match_id, packet_url=packet_url)


@router.get("/{job_id}/packets.zip")
async def download_packets_zip(job_id: str):
    pdir = storage.packets_dir(job_id)
    pdfs = sorted(pdir.glob("*.pdf"))
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
