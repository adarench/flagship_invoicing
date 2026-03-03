# Flagship Invoicing — Invoice Reconciliation Pipeline

Automated reconciliation of project invoices (PID) against Northern Trust bank statements. Extracts, matches, and reports on check payments across multiple fiscal years.

---

## Overview

The pipeline ingests two data sources:

- **PID** (`data/pid_raw/`) — Project Invoice Detail spreadsheet listing expected payments by vendor, amount, and date
- **Bank PDFs** (`data/bank_raw/`) — Northern Trust bank statements (one PDF per year per account)

It produces:

- `output/reconciled.xlsx` — Matched PID ↔ bank records with match type and confidence
- `output/unmatched.xlsx` — Remaining unmatched records from both sides

---

## How It Works

### Stage 1 — Parse PID
Reads the PID Excel file and normalizes vendor names, amounts, dates, and check numbers.

### Stage 2 — Parse Bank PDFs
Northern Trust statements have three page types per monthly period:
1. **Cover / reconciliation page** — skipped
2. **Check register** — parsed via text regex (Date | Check No. | Amount columns)
3. **Scanned check face images** — parsed via Claude Sonnet vision OCR (MVP 2)

The LLM fallback (`src/ingest/llm_pdf_fallback.py`) detects pages containing embedded 1-bit PNG check images, extracts the highest-resolution image directly, and sends it to Claude Sonnet to extract `check_no`, `date`, `amount`, and `vendor`.

### Stage 3 — Harmonize & Canonicalize
Records are normalized to a common schema. Claude Haiku fuzzy-matches bank vendor strings (often OCR noise or abbreviations) to canonical PID vendor names.

### Stage 4 — Deterministic Matching
Three match tiers, in priority order:
| Tier | Logic |
|------|-------|
| **Primary** | `check_no` + `amount` exact match within date window |
| **Secondary** | `date` + `amount` exact match (no check number) |
| **Retention** | Single PID entry matched to multiple bank debits (split payments) |
| **Fuzzy** | `canonical_vendor` + `amount` match after Haiku canonicalization |

### Stage 5 — Report
Generates `reconciled.xlsx` and `unmatched.xlsx`.

### Stage 6 — Packet Reasoning (MVP 3)
Generates cached audit reasoning text for matched records (`output/packet_reasoning.json`).

---

## Setup

```bash
# 1. Clone and enter the repo
git clone https://github.com/adarench/flagship_invoicing.git
cd flagship_invoicing

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure API key
cp .env.example .env
# Edit .env and set ANTHROPIC_API_KEY=sk-ant-...

# 4. Add data files
#   data/pid_raw/   ← PID Excel file (.xlsx)
#   data/bank_raw/  ← Northern Trust PDF statements
```

---

## Running

```bash
# Run the full pipeline
python run_pipeline.py

# Run individual stages
python run_pipeline.py --stage parse_pid
python run_pipeline.py --stage parse_banks
python run_pipeline.py --stage match
python run_pipeline.py --stage report
python run_pipeline.py --stage fuzzy_match
python run_pipeline.py --stage llm_parse_fallback
python run_pipeline.py --stage packet_reasoning

# Point to specific files
python run_pipeline.py --pid path/to/PID.xlsx --banks path/to/bank_pdfs/
```

---

## Project Structure

```
.
├── config.py                        # Paths, model IDs, API key
├── run_pipeline.py                  # Pipeline entry point
├── requirements.txt
│
├── src/ingest/
│   ├── parse_pid.py                 # PID Excel → normalized CSV
│   ├── parse_bank_pdf.py            # Bank PDF → transactions CSV
│   └── llm_pdf_fallback.py          # Claude Sonnet OCR for check images
│
├── match/
│   ├── harmonize_records.py         # Load + normalize both sources
│   ├── deterministic.py             # Primary / secondary / retention / fuzzy tiers
│   └── fuzzy_llm.py                 # Claude Haiku vendor canonicalization
│
├── output/
│   ├── reconciliation_report.py     # Excel report generation
│   ├── generate_packet.py           # 3-page per-record PDF packet generation
│   └── packet_reasoning_llm.py      # LLM audit reasoning + cache
│
├── api/                             # FastAPI backend
└── web/                             # Next.js portal
│
└── data/                            # Excluded from repo (add locally)
    ├── pid_raw/
    ├── bank_raw/
    └── interim/
```

---

## Current Results (FLAG bank, 3 fiscal years)

| Metric | Value |
|--------|-------|
| PID records | 790 |
| Bank transactions | 820 |
| **Total matched** | **125 (15.8%)** |
| — Primary (check_no + amount) | 108 |
| — Fuzzy vendor | 10 |
| — Retention / split | 5 |
| — Secondary (date + amount) | 2 |
| Unmatched | 665 |

Match rate will increase as AG, WDCPL, and GPM statements are added and coverage improves.

---

## Local Services

```bash
# Backend API
uvicorn api.main:app --reload --port 8000

# Frontend portal
cd web
npm run dev
```
