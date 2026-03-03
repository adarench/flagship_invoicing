from __future__ import annotations
"""
api/db.py — SQLite job state management.

Tables:
  jobs  : job_id, created_at, updated_at, state, progress, error_message
  steps : job_id, step_name, status
"""

import sqlite3
import threading
from pathlib import Path
from datetime import datetime, timezone

STORAGE_DIR = Path(__file__).parent.parent / "storage"
DB_PATH = STORAGE_DIR / "jobs.db"

STEPS = [
    "parse_pid",
    "parse_banks",
    "canonicalize_vendors",
    "match",
    "report",
    "build_artifacts",
]

_lock = threading.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_conn() -> sqlite3.Connection:
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create tables if they don't exist."""
    with _get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id        TEXT PRIMARY KEY,
                created_at    TEXT NOT NULL,
                updated_at    TEXT NOT NULL,
                state         TEXT NOT NULL DEFAULT 'pending',
                progress      INTEGER NOT NULL DEFAULT 0,
                error_message TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS steps (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id    TEXT NOT NULL,
                step_name TEXT NOT NULL,
                status    TEXT NOT NULL DEFAULT 'pending',
                UNIQUE(job_id, step_name)
            )
        """)
        conn.commit()


def mark_stale_running_jobs() -> int:
    """
    Mark jobs left in a running/pending state from a previous process as error.
    Returns number of jobs updated.
    """
    now = _now()
    message = "Job interrupted before completion (service restart or crash)"
    with _lock, _get_conn() as conn:
        rows = conn.execute(
            "SELECT job_id FROM jobs WHERE state IN ('running', 'pending')"
        ).fetchall()
        job_ids = [r["job_id"] for r in rows]
        if not job_ids:
            return 0

        conn.executemany(
            "UPDATE jobs SET state='error', error_message=?, updated_at=? WHERE job_id=?",
            [(message, now, jid) for jid in job_ids],
        )
        conn.executemany(
            "UPDATE steps SET status='error' WHERE job_id=? AND status='running'",
            [(jid,) for jid in job_ids],
        )
        conn.commit()
        return len(job_ids)


def create_job(job_id: str) -> None:
    """Insert a new job record and its default steps."""
    now = _now()
    with _lock, _get_conn() as conn:
        conn.execute(
            "INSERT INTO jobs (job_id, created_at, updated_at, state, progress) VALUES (?, ?, ?, 'pending', 0)",
            (job_id, now, now),
        )
        for step in STEPS:
            conn.execute(
                "INSERT OR IGNORE INTO steps (job_id, step_name, status) VALUES (?, ?, 'pending')",
                (job_id, step),
            )
        conn.commit()


def update_job(
    job_id: str,
    state: str | None = None,
    progress: int | None = None,
    error_message: str | None = None,
) -> None:
    """Update job state/progress."""
    fields: list[str] = ["updated_at = ?"]
    params: list = [_now()]
    if state is not None:
        fields.append("state = ?")
        params.append(state)
    if progress is not None:
        fields.append("progress = ?")
        params.append(progress)
    if error_message is not None:
        fields.append("error_message = ?")
        params.append(error_message)
    params.append(job_id)

    with _lock, _get_conn() as conn:
        conn.execute(
            f"UPDATE jobs SET {', '.join(fields)} WHERE job_id = ?",
            params,
        )
        conn.commit()


def update_step(job_id: str, step_name: str, status: str) -> None:
    """Update a single step's status."""
    with _lock, _get_conn() as conn:
        conn.execute(
            "UPDATE steps SET status = ? WHERE job_id = ? AND step_name = ?",
            (status, job_id, step_name),
        )
        conn.commit()


def get_job(job_id: str):
    """Return job dict with steps list, or None if not found."""
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
        if row is None:
            return None
        job = dict(row)
        steps = conn.execute(
            "SELECT step_name, status FROM steps WHERE job_id = ? ORDER BY id",
            (job_id,),
        ).fetchall()
        job["steps"] = [dict(s) for s in steps]
        return job


def list_jobs() -> list[dict]:
    """Return all jobs ordered by created_at desc."""
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM jobs ORDER BY created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]
