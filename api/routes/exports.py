from __future__ import annotations
"""
api/routes/exports.py

GET /api/jobs/{job_id}/exports/reconciled.xlsx
GET /api/jobs/{job_id}/exports/unmatched.xlsx
GET /api/jobs/{job_id}/exports/summary.json
GET /api/jobs/{job_id}/exports/raw_ocr.json
GET /api/jobs/{job_id}/exports/packet_manifest.json
GET /api/jobs/{job_id}/exports/packets/{filename}  — individual PDF packet
GET /api/jobs/{job_id}/log                          — streaming run log
GET /api/jobs/{job_id}/pdf/{filename}               — render bank PDF page to PNG
"""

import io
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse, PlainTextResponse

from api import storage
from api.models import PDFSourcesResponse, PDFSource

router = APIRouter()


def _require_file(job_id: str, filename: str) -> Path:
    path = storage.artifact_path(job_id, filename)
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Export '{filename}' not yet available for job {job_id!r}",
        )
    return path


@router.get("/{job_id}/exports/reconciled.xlsx")
async def download_reconciled(job_id: str):
    path = _require_file(job_id, "reconciled.xlsx")
    return FileResponse(
        path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="reconciled.xlsx",
    )


@router.get("/{job_id}/exports/unmatched.xlsx")
async def download_unmatched(job_id: str):
    path = _require_file(job_id, "unmatched.xlsx")
    return FileResponse(
        path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="unmatched.xlsx",
    )


@router.get("/{job_id}/exports/summary.json")
async def download_summary_json(job_id: str):
    path = _require_file(job_id, "summary.json")
    return FileResponse(path, media_type="application/json", filename="summary.json")


@router.get("/{job_id}/exports/raw_ocr.json")
async def download_raw_ocr(job_id: str):
    path = _require_file(job_id, "raw_ocr.json")
    return FileResponse(path, media_type="application/json", filename="raw_ocr.json")


@router.get("/{job_id}/exports/packet_manifest.json")
async def download_packet_manifest(job_id: str):
    path = _require_file(job_id, "packet_manifest.json")
    return FileResponse(path, media_type="application/json", filename="packet_manifest.json")


@router.get("/{job_id}/exports/packets/{filename}")
async def download_packet(job_id: str, filename: str):
    path = storage.packets_dir(job_id) / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Packet '{filename}' not found")
    return FileResponse(path, media_type="application/pdf", filename=filename)


@router.get("/{job_id}/log")
async def get_log(job_id: str):
    path = storage.log_path(job_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Log not found")
    return PlainTextResponse(path.read_text())


@router.get("/{job_id}/pdf/{filename}")
async def render_pdf_page(
    job_id: str,
    filename: str,
    page: int = Query(default=0, ge=0),
):
    """Render a single page of a bank PDF to PNG and return the image bytes."""
    # Try to find the PDF in uploads or storage
    pdf_path = storage.uploads_dir(job_id) / filename
    if not pdf_path.exists():
        pdf_path = storage.artifact_path(job_id, filename)
    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail=f"PDF '{filename}' not found for job {job_id!r}")

    try:
        import fitz  # PyMuPDF
    except ImportError:
        raise HTTPException(status_code=500, detail="PyMuPDF not installed — cannot render PDF pages")

    doc = fitz.open(str(pdf_path))
    try:
        if page >= len(doc):
            raise HTTPException(
                status_code=400,
                detail=f"Page {page} out of range (document has {len(doc)} pages)",
            )

        pg = doc[page]
        mat = fitz.Matrix(2.0, 2.0)   # 2× scale for legibility
        pix = pg.get_pixmap(matrix=mat)
        img_bytes = pix.tobytes("png")
    finally:
        doc.close()

    return StreamingResponse(
        io.BytesIO(img_bytes),
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@router.get("/{job_id}/pdf_sources", response_model=PDFSourcesResponse)
async def list_pdf_sources(job_id: str):
    upload_dir = storage.uploads_dir(job_id)
    pdfs = sorted(upload_dir.glob("*.pdf"))
    sources: list[PDFSource] = []

    try:
        import fitz  # PyMuPDF
    except ImportError:
        raise HTTPException(status_code=500, detail="PyMuPDF not installed")

    for pdf in pdfs:
        page_count = 0
        try:
            doc = fitz.open(str(pdf))
            page_count = len(doc)
            doc.close()
        except Exception:
            page_count = 0
        sources.append(PDFSource(filename=pdf.name, page_count=page_count))

    return PDFSourcesResponse(job_id=job_id, sources=sources)
