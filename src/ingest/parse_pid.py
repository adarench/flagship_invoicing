from __future__ import annotations
"""
src/ingest/parse_pid.py

Reads PID.xlsx from data/pid_raw/, auto-detects column mapping,
normalizes to canonical schema, outputs to data/interim/pid.csv.

Canonical schema:
    pid_id       str
    vendor       str
    invoice_no   str
    invoice_date date
    amount       float
    check_no     str
    check_date   date
    bank         str
    phase        str
    reference    str
"""

import re
import sys
import logging
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import DATA_PID_RAW, DATA_INTERIM

logger = logging.getLogger(__name__)

# Canonical field → possible column name aliases (lowercase, stripped)
COLUMN_ALIASES: dict[str, list[str]] = {
    "pid_id": [
        "pid_id", "pid id", "pid", "project id", "project_id",
        "id", "record id", "rec id", "record no",
    ],
    "vendor": [
        "vendor", "payee", "vendor name", "company", "contractor",
        "vendor/payee", "name", "paid to",
    ],
    "invoice_no": [
        "invoice_no", "invoice no", "invoice number", "invoice #",
        "inv no", "inv#", "inv num", "invoice",
    ],
    "invoice_date": [
        "invoice date", "invoice_date", "inv date", "date invoiced",
        "invoice dt", "date of invoice",
    ],
    "amount": [
        "amount", "amt", "total", "invoice amount", "check amount",
        "payment amount", "net amount", "gross amount", "dollar amount",
    ],
    "check_no": [
        "check no", "check_no", "check number", "check #", "chk no",
        "chk#", "chk num", "check", "ck no", "ck#", "ck #",
    ],
    "check_date": [
        "check date", "check_date", "date paid", "paid date",
        "payment date", "date issued", "issue date", "date of check",
        "ck date", "ck_date",
    ],
    "bank": [
        "bank", "bank name", "financial institution", "fund",
        "account", "bank account", "bank acct",
    ],
    "phase": [
        "phase", "project phase", "construction phase", "phase no",
        "phase number", "phase #",
    ],
    "reference": [
        "reference", "ref", "reference no", "ref no", "memo",
        "project", "job no", "job number", "job", "contract no",
        "po number", "po no", "po#",
    ],
}


def _normalize_col(col: str) -> str:
    """Lowercase, strip, collapse whitespace."""
    return re.sub(r"\s+", " ", str(col).lower().strip())


def _detect_column_mapping(df_columns: list[str]) -> dict[str, str]:
    """
    Given actual DataFrame columns, return mapping: canonical_field → actual_column.
    Uses exact alias match first, then substring fallback.
    """
    normalized = {_normalize_col(c): c for c in df_columns}
    mapping: dict[str, str] = {}
    unmatched: list[str] = []

    for canonical, aliases in COLUMN_ALIASES.items():
        found = False
        # Pass 1: exact match on normalized alias
        for alias in aliases:
            if alias in normalized:
                mapping[canonical] = normalized[alias]
                found = True
                break
        if found:
            continue
        # Pass 2: substring match (alias contained in column name or vice versa)
        for alias in aliases:
            for norm_col, orig_col in normalized.items():
                if alias in norm_col or norm_col in alias:
                    mapping[canonical] = orig_col
                    found = True
                    break
            if found:
                break
        if not found:
            unmatched.append(canonical)

    if unmatched:
        logger.warning(f"Could not map canonical fields: {unmatched}")
        logger.warning(f"Available columns: {list(df_columns)}")

    return mapping


def _parse_amount(val) -> float | None:
    """Parse amount string → float. Handles $, commas, parentheses for negatives."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "-", ""):
        return None
    negative = s.startswith("(") and s.endswith(")")
    s = s.strip("()")
    s = re.sub(r"[$,\s]", "", s)
    try:
        result = float(s)
        return -result if negative else result
    except ValueError:
        logger.debug(f"Could not parse amount: {val!r}")
        return None


def _parse_date(val) -> pd.Timestamp | None:
    """Parse various date formats to pandas Timestamp."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, pd.Timestamp):
        return val
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", ""):
        return None
    try:
        return pd.to_datetime(s, infer_datetime_format=True)
    except Exception:
        logger.debug(f"Could not parse date: {val!r}")
        return None


def _clean_check_no(val) -> str:
    """Strip whitespace; remove trailing .0 from float-parsed integers."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip()
    if s.lower() in ("nan", "none"):
        return ""
    if s.endswith(".0"):
        s = s[:-2]
    return s.strip()


def _parse_sheet_skip_empty_header(xl: pd.ExcelFile, sheet: str) -> pd.DataFrame:
    """
    Parse one Excel sheet, skipping empty leading rows to find the real header.
    Uses the same density heuristic as _read_csv_skip_empty_header.
    """
    raw = xl.parse(sheet, header=None, dtype=str)
    header_row_idx = None
    for i, row in raw.iterrows():
        non_empty = [c for c in row if c and str(c).strip().lower() not in ("nan", "none", "")]
        if len(non_empty) >= max(3, len(row) * 0.3):
            header_row_idx = i
            break
    if header_row_idx is None:
        return pd.DataFrame()
    headers = [str(c).strip() for c in raw.iloc[header_row_idx]]
    data_df = raw.iloc[header_row_idx + 1:].copy()
    data_df.columns = headers
    data_df = data_df.reset_index(drop=True)
    logger.info(f"Excel header detected at row {header_row_idx} in sheet '{sheet}': {headers}")
    return data_df


def _pick_best_sheet(xl: pd.ExcelFile) -> pd.DataFrame:
    """Return the sheet with the most data rows, with empty-leading-row detection."""
    best_df = pd.DataFrame()
    best_name = None
    for sheet in xl.sheet_names:
        try:
            df = _parse_sheet_skip_empty_header(xl, sheet)
            if len(df) > len(best_df):
                best_df = df
                best_name = sheet
        except Exception:
            continue
    if best_name:
        logger.info(f"Using sheet '{best_name}' with {len(best_df)} rows")
    return best_df


def _read_csv_skip_empty_header(csv_path: Path) -> pd.DataFrame:
    """
    Read a CSV that may have empty leading rows before the real header row.
    Scans downward until it finds the first row where any cell has content,
    treats that row as the header, and returns the data below it.
    """
    raw = pd.read_csv(csv_path, header=None, dtype=str, encoding="utf-8-sig")
    header_row_idx = None
    for i, row in raw.iterrows():
        non_empty = [c for c in row if c and str(c).strip().lower() not in ("nan", "none", "")]
        # Require >= 30% of cells to be filled — avoids partial rows like "Added by SPA"
        if len(non_empty) >= max(3, len(row) * 0.3):
            header_row_idx = i
            break

    if header_row_idx is None:
        logger.warning(f"Could not detect header row in {csv_path.name}")
        return pd.DataFrame()

    headers = [str(c).strip() for c in raw.iloc[header_row_idx]]
    data_df = raw.iloc[header_row_idx + 1:].copy()
    data_df.columns = headers
    data_df = data_df.reset_index(drop=True)
    logger.info(
        f"CSV header detected at row {header_row_idx}: {headers}"
    )
    return data_df


def parse_pid(pid_path: Path | None = None) -> pd.DataFrame:
    """
    Parse PID file (Excel or CSV) → normalized DataFrame.

    If pid_path is None, scans DATA_PID_RAW for .xlsx, .xls, or .csv files.
    Column names are auto-detected; see COLUMN_ALIASES for recognized variants.
    """
    if pid_path is None:
        candidates = (
            list(DATA_PID_RAW.glob("*.xlsx")) +
            list(DATA_PID_RAW.glob("*.xls")) +
            list(DATA_PID_RAW.glob("*.csv"))
        )
        if not candidates:
            raise FileNotFoundError(
                f"No PID file found in {DATA_PID_RAW}. "
                "Drop a .xlsx, .xls, or .csv file there and re-run."
            )
        pid_path = candidates[0]
        if len(candidates) > 1:
            logger.warning(f"Multiple PID files found — using: {pid_path.name}")

    logger.info(f"Reading PID file: {pid_path}")

    suffix = pid_path.suffix.lower()
    if suffix == ".csv":
        raw_df = _read_csv_skip_empty_header(pid_path)
    else:
        xl = pd.ExcelFile(pid_path)
        raw_df = _pick_best_sheet(xl)

    if raw_df.empty:
        raise ValueError(f"PID file appears empty: {pid_path}")

    logger.info(f"Raw columns: {list(raw_df.columns)}")

    col_map = _detect_column_mapping(list(raw_df.columns))
    logger.info(f"Column mapping: {col_map}")

    # Build canonical output row-by-row
    out_rows = []
    for idx, row in raw_df.iterrows():

        def get(field: str, default="") -> str:
            col = col_map.get(field)
            return str(row[col]).strip() if col else default

        # pid_id: use mapped column or synthesize from index
        pid_raw = get("pid_id")
        pid_id = pid_raw if pid_raw and pid_raw.lower() not in ("nan", "none", "") else f"PID_{idx:04d}"

        amount = _parse_amount(row[col_map["amount"]]) if "amount" in col_map else None

        # Skip rows with no parseable amount (blank/footer rows)
        if amount is None or amount == 0:
            continue

        out_rows.append({
            "pid_id":       pid_id,
            "vendor":       get("vendor"),
            "invoice_no":   get("invoice_no"),
            "invoice_date": _parse_date(row[col_map["invoice_date"]]) if "invoice_date" in col_map else None,
            "amount":       amount,
            "check_no":     _clean_check_no(row[col_map["check_no"]] if "check_no" in col_map else None),
            "check_date":   _parse_date(row[col_map["check_date"]]) if "check_date" in col_map else None,
            "bank":         get("bank"),
            "phase":        get("phase"),
            "reference":    get("reference"),
        })

    df = pd.DataFrame(out_rows).reset_index(drop=True)
    logger.info(f"Parsed {len(df)} PID records")
    return df


def save_pid(df: pd.DataFrame, output_path: Path | None = None) -> Path:
    """Save normalized PID DataFrame to CSV."""
    if output_path is None:
        output_path = DATA_INTERIM / "pid.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved PID CSV → {output_path}")
    return output_path


def run(
    pid_path: Path | None = None,
    output_path: Path | None = None,
) -> pd.DataFrame:
    """Parse PID file (xlsx or csv) and save to pid.csv."""
    df = parse_pid(pid_path)
    save_pid(df, output_path=output_path)
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    df = run()
    print(df.head(10).to_string())
    print(f"\nShape: {df.shape}")
    print(f"\nDtypes:\n{df.dtypes}")
