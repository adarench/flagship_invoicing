from __future__ import annotations
"""
api/main.py — FastAPI application entry point.

Start with:
    uvicorn api.main:app --reload --port 8000
"""

import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api import db
from api.routes import jobs, summary, review, packets, exports

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Flagborough Finance Reconciliation API",
    version="1.0.0",
    description="Backend API for the Invoice Reconciliation Portal",
)

# ── CORS ───────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Startup ────────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    db.init_db()
    recovered = db.mark_stale_running_jobs()
    logger.info("Database initialized")
    if recovered:
        logger.warning(f"Marked {recovered} stale running/pending jobs as error")
    # Ensure storage directory exists
    (Path(__file__).parent.parent / "storage").mkdir(parents=True, exist_ok=True)
    logger.info("Flagborough Reconciliation API ready")


# ── Routers ────────────────────────────────────────────────────────────────────
app.include_router(jobs.router,    prefix="/api/jobs",  tags=["jobs"])
app.include_router(summary.router, prefix="/api/jobs",  tags=["summary"])
app.include_router(review.router,  prefix="/api/jobs",  tags=["review"])
app.include_router(packets.router, prefix="/api/jobs",  tags=["packets"])
app.include_router(exports.router, prefix="/api/jobs",  tags=["exports"])


@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "flagborough-reconciliation-api"}
