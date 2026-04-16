"""Read data from existing Google Sheets tabs."""
import logging
import re

import gspread

from config.constants import (
    OD_TABLE_SHEET_NAME,
    SOURCE_SHEET_NAME,
    SPREADSHEET_ID,
)
from src.sheets.models import ExistingSheetRow

logger = logging.getLogger(__name__)


def _safe_float(val: str | None) -> float | None:
    """Parse a float from a cell value, returning None on failure."""
    if not val:
        return None
    val = str(val).strip().replace(",", "")
    if val.startswith("#") or val == "":
        return None
    try:
        return float(val)
    except ValueError:
        return None


def get_gspread_client() -> gspread.Client:
    """Get a gspread client using default service account credentials."""
    return gspread.service_account()


def read_chs_jis_m(gc: gspread.Client) -> list[ExistingSheetRow]:
    """Read the 'CHS JIS M' sheet and parse into structured rows."""
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(SOURCE_SHEET_NAME)
    all_rows = ws.get_all_values()

    if len(all_rows) < 4:
        logger.warning("CHS JIS M sheet has fewer than 4 rows")
        return []

    # Data starts at row 5 (index 4) — rows 1-3 are headers
    results = []
    for i, row in enumerate(all_rows[3:], start=4):
        model = row[0].strip() if row[0] else ""
        if not model:
            continue

        results.append(
            ExistingSheetRow(
                model=model,
                date=row[1].strip() if len(row) > 1 and row[1] else None,
                osh_code=row[2].strip() if len(row) > 2 and row[2] else None,
                description=row[7].strip() if len(row) > 7 and row[7] else None,
                weight_kg_6m=_safe_float(row[9] if len(row) > 9 else None),
                weight_kg_per_m=_safe_float(row[13] if len(row) > 13 else None),
                price_per_kg=_safe_float(row[14] if len(row) > 14 else None),
                diameter_mm=_safe_float(row[15] if len(row) > 15 else None),
                diameter_inch=row[16].strip() if len(row) > 16 and row[16] else None,
                thickness_mm=_safe_float(row[19] if len(row) > 19 else None),
                price_thb=_safe_float(row[22] if len(row) > 22 else None),
                cost_thb=_safe_float(row[24] if len(row) > 24 else None),
            )
        )

    logger.info(f"Read {len(results)} rows from {SOURCE_SHEET_NAME}")
    return results


def read_chs_table(gc: gspread.Client) -> dict[str, float]:
    """Read the 'CHS Table' sheet and return inch→OD mapping for TIS/JIS.

    Returns a dict like {"1/2": 21.7, "3/4": 27.2, ...}
    """
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(OD_TABLE_SHEET_NAME)
    all_rows = ws.get_all_values()

    od_map: dict[str, float] = {}
    for row in all_rows[1:]:  # skip header
        inch = row[0].strip() if row[0] else ""
        tis_jis_od = _safe_float(row[1] if len(row) > 1 else None)
        if inch and tis_jis_od:
            od_map[inch] = tis_jis_od

    logger.info(f"Read {len(od_map)} OD entries from {OD_TABLE_SHEET_NAME}")
    return od_map
