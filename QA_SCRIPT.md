# Invoice Reconciliation - End-to-End QA Script

This script verifies the full workflow:
Upload -> Progress -> Dashboard -> Review -> Match Detail -> Packets -> Exports -> History -> Re-run.

## 1) Environment Setup

```bash
# from repo root
pip install -r requirements.txt
cp .env.example .env
# set ANTHROPIC_API_KEY in .env

cd web
npm install
cd ..
```

## 2) Start Services

Terminal A:
```bash
uvicorn api.main:app --reload --port 8000
```

Terminal B:
```bash
cd web
NEXT_PUBLIC_API_URL=http://127.0.0.1:8000 npm run dev
```

Open `http://127.0.0.1:3000`.

## 3) Upload Flow

1. Go to `/upload`.
2. Upload one PID spreadsheet (`.xlsx/.xls/.csv`).
3. Upload one or more bank PDFs (`.pdf`).
4. Click `Start Reconciliation`.
5. Confirm redirect to `/job/<job_id>/progress`.

Expected:
- Progress bar increments.
- Step states move from `pending` -> `running` -> `done`.
- Run log updates every few seconds.

## 4) Progress and Completion

1. Wait for job state `completed`.
2. Confirm auto-redirect to `/dashboard/<job_id>`.

Expected:
- No error banner.
- Completion reaches 100%.

## 5) Dashboard Checks

On `/dashboard/<job_id>` verify:
- KPI cards render (match rate, matched, unmatched, total amount).
- Match breakdown chart renders all categories.
- Coverage matrix shows bank/month counts.
- Review/Exports buttons navigate correctly.

## 6) Review Queue Checks

1. Go to `/review/<job_id>`.
2. Use search and dropdown filters.
3. Open at least one row.

Expected:
- Counts for pending/approved/rejected update correctly.
- Row click navigates to detail page.

## 7) Match Detail + PDF Viewer

1. On `/review/<job_id>/detail/<match_id>`, click `View PDF`.
2. If multiple PDFs exist, switch source from dropdown.
3. Navigate pages and zoom in/out.

Expected:
- PDF page image renders.
- Page controls respect page bounds.
- No hardcoded filename/page assumptions.

## 8) Approve/Reject Actions

1. On match detail, click `Approve` (or `Reject`).
2. Return to queue page and verify status/counters update.

Expected:
- API call succeeds.
- Status badge reflects change.
- No stale UI values.

## 9) Packet Generation

1. Go to `/packets/<job_id>`.
2. Click `Generate Packet` on a few records.
3. Download one generated PDF.
4. Click `Download All (.zip)`.

Expected:
- Each generated packet downloads.
- Packet is a 3-page PDF:
  - Page 1 PID + match metadata
  - Page 2 bank evidence rows
  - Page 3 reasoning text
- ZIP contains generated packet files.

## 10) Exports Verification

Go to `/exports/<job_id>` and download:
- `reconciled.xlsx`
- `unmatched.xlsx`
- `summary.json`
- `raw_ocr.json`
- `packet_manifest.json` (after generating at least one packet)

Expected:
- Files download successfully.
- `summary.json` contains expected aggregate fields.
- `raw_ocr.json` includes OCR/fallback metadata.

## 11) History and Re-run

1. Go to `/history`.
2. Confirm new job appears with `completed` status.
3. Start another job from `New Job`.
4. Confirm both historical jobs remain visible.

Expected:
- No loss of prior job artifacts.
- New job runs independently.

## 12) API Smoke Checks (optional)

```bash
JOB=<job_id>
BASE=http://127.0.0.1:8000

curl -s $BASE/api/health
curl -s $BASE/api/jobs/$JOB/status
curl -s $BASE/api/jobs/$JOB/summary
curl -s $BASE/api/jobs/$JOB/coverage
curl -s $BASE/api/jobs/$JOB/review_queue
curl -s $BASE/api/jobs/$JOB/pdf_sources
curl -I $BASE/api/jobs/$JOB/exports/reconciled.xlsx
curl -I $BASE/api/jobs/$JOB/exports/unmatched.xlsx
curl -I $BASE/api/jobs/$JOB/exports/summary.json
curl -I $BASE/api/jobs/$JOB/exports/raw_ocr.json
curl -I $BASE/api/jobs/$JOB/packets.zip
```

Expected:
- JSON routes return 200 with valid shape.
- Export routes return 200 when artifacts exist.
- Packet zip returns 404 before packet generation, 200 after generation.
