from __future__ import annotations
"""
api/storage.py — Job artifact I/O.

Each job gets its own directory: storage/<job_id>/
Artifacts written there:
  uploads/       raw uploaded files
  pid.csv
  bank_FLAG.csv, bank_AG.csv, ...
  reconciled.xlsx
  unmatched.xlsx
  summary.json
  coverage.json
  review_queue.json
  raw_ocr.json
  packets/
  run.log
"""

import json
import shutil
from pathlib import Path

STORAGE_ROOT = Path(__file__).parent.parent / "storage"


def job_dir(job_id: str) -> Path:
    d = STORAGE_ROOT / job_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def uploads_dir(job_id: str) -> Path:
    d = job_dir(job_id) / "uploads"
    d.mkdir(parents=True, exist_ok=True)
    return d


def packets_dir(job_id: str) -> Path:
    d = job_dir(job_id) / "packets"
    d.mkdir(parents=True, exist_ok=True)
    return d


def artifact_path(job_id: str, filename: str) -> Path:
    return job_dir(job_id) / filename


def write_json(job_id: str, filename: str, data: object) -> Path:
    path = artifact_path(job_id, filename)
    path.write_text(json.dumps(data, indent=2, default=str))
    return path


def read_json(job_id: str, filename: str) -> object:
    path = artifact_path(job_id, filename)
    if not path.exists():
        raise FileNotFoundError(f"Artifact not found: {path}")
    return json.loads(path.read_text())


def artifact_exists(job_id: str, filename: str) -> bool:
    return artifact_path(job_id, filename).exists()


def copy_to_job(job_id: str, src: Path, dest_name: str | None = None) -> Path:
    dest = artifact_path(job_id, dest_name or src.name)
    shutil.copy2(src, dest)
    return dest


def log_path(job_id: str) -> Path:
    return artifact_path(job_id, "run.log")


def append_log(job_id: str, line: str) -> None:
    with open(log_path(job_id), "a") as f:
        f.write(line + "\n")
