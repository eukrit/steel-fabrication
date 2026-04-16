"""Write data to the 'CHS JIS Claude' output sheet."""
import logging

import gspread

from config.constants import OUTPUT_SHEET_NAME, SPREADSHEET_ID
from src.sheets.models import OutputSheetRow

logger = logging.getLogger(__name__)


def write_chs_jis_claude(
    gc: gspread.Client, rows: list[OutputSheetRow]
) -> None:
    """Create or overwrite the 'CHS JIS Claude' sheet with merged data."""
    sh = gc.open_by_key(SPREADSHEET_ID)

    # Find or create the output worksheet
    try:
        ws = sh.worksheet(OUTPUT_SHEET_NAME)
        ws.clear()
        logger.info(f"Cleared existing '{OUTPUT_SHEET_NAME}' sheet")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(
            title=OUTPUT_SHEET_NAME, rows=len(rows) + 10, cols=20
        )
        logger.info(f"Created new '{OUTPUT_SHEET_NAME}' sheet")

    # Write header
    header = OutputSheetRow.header_row()
    ws.append_row(header, value_input_option="RAW")

    # Write data rows in batches
    batch = [r.to_row() for r in rows]
    if batch:
        ws.append_rows(batch, value_input_option="RAW")

    # Format header row: bold + freeze
    ws.format("1:1", {"textFormat": {"bold": True}})
    ws.freeze(rows=1)

    logger.info(
        f"Wrote {len(rows)} rows to '{OUTPUT_SHEET_NAME}' sheet"
    )
