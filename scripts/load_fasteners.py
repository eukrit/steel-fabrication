"""Load Fasteners Schedule Ver. 2024 spreadsheet into Firestore.

Source sheet: https://docs.google.com/spreadsheets/d/1VjEG_KSlDcfK6DO8pm_UIrZkUkawixPwzSVvGwKv_Lk

Firestore database: steel-sections (reused)

Collections written
-------------------
- fasteners                       Master catalog (one doc per model) + `orders` subcollection
- fastener_types                  Reference: type → material + Thai description
- fastener_threads                Reference: thread size → pitch
- fastener_config                 Assembly config: clamp/base/anchor bolt hardware sets
- fastener_fittings               Keder / cable / fork fittings rows
- fastener_pricelist              Simple pricelist (ANC SUS, Hilti HY 200R, etc.)
- fastener_vendor_pricelists      Vendor pivot pricelists (TPC, Abpon)
- fastener_orders                 Order metadata (project + date per Order N column)
- fastener_purchase_orders        SO21/SO22/SO23 purchase order documents
- fastener_total_orders           Flat rollup from "Total Order" tab
- fastener_sync_runs              Audit log of loader runs

Usage
-----
    python scripts/load_fasteners.py               # load everything
    python scripts/load_fasteners.py --dry-run     # parse + print counts only
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import gspread
from google.cloud import firestore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("load_fasteners")

SPREADSHEET_ID = "1VjEG_KSlDcfK6DO8pm_UIrZkUkawixPwzSVvGwKv_Lk"
GCP_PROJECT_ID = "ai-agents-go"
FIRESTORE_DATABASE = "steel-sections"

# ---------- helpers ----------

def _sa_credentials_path() -> str:
    """Resolve the service-account JSON path for gspread + Firestore."""
    env = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if env and Path(env).exists():
        return env
    creds_dir = Path("C:/Users/Eukrit/OneDrive/Documents/Claude Code/Credentials Claude Code")
    candidates = sorted(creds_dir.glob("ai-agents-go-*.json"))
    if candidates:
        return str(candidates[-1]).replace("\\", "/")
    raise FileNotFoundError(
        "ai-agents-go SA key not found. "
        "Set GOOGLE_APPLICATION_CREDENTIALS or place an ai-agents-go-*.json in the credentials folder."
    )


def _slug(s: str) -> str:
    """Firestore-safe, lowercase slug for use as a document ID."""
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s or "unnamed"


def _to_float(val: Any) -> float | None:
    if val is None:
        return None
    s = str(val).strip().replace(",", "").replace("%", "")
    if not s or s.startswith("#") or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _to_int(val: Any) -> int | None:
    f = _to_float(val)
    return int(f) if f is not None else None


def _clean(s: Any) -> str:
    return str(s).strip() if s is not None else ""


def _parse_length_from_model(model: str) -> float | None:
    """Extract the length in mm from a model string like 'BOLT SUS M12-1.75x40'."""
    m = re.search(r"x\s*(\d+(?:\.\d+)?)\s*$", model)
    return float(m.group(1)) if m else None


def _batch_commit(db: firestore.Client, operations: list[tuple[firestore.DocumentReference, dict]]) -> int:
    """Commit a list of (doc_ref, payload) set operations in 400-sized batches."""
    written = 0
    batch = db.batch()
    n = 0
    for ref, payload in operations:
        batch.set(ref, payload, merge=True)
        n += 1
        written += 1
        if n >= 400:
            batch.commit()
            batch = db.batch()
            n = 0
    if n > 0:
        batch.commit()
    return written


# ---------- clients ----------

def get_clients() -> tuple[gspread.Client, firestore.Client]:
    sa_path = _sa_credentials_path()
    gc = gspread.service_account(filename=sa_path)
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", sa_path)
    db = firestore.Client(project=GCP_PROJECT_ID, database=FIRESTORE_DATABASE)
    logger.info("clients ready: spreadsheet=%s firestore_db=%s", SPREADSHEET_ID, FIRESTORE_DATABASE)
    return gc, db


class SheetCache:
    """Fetch every worksheet's values once, with retry-on-429 backoff."""

    def __init__(self, gc: gspread.Client, spreadsheet_id: str):
        self.sh = self._with_retry(lambda: gc.open_by_key(spreadsheet_id))
        self._cache: dict[str, list[list[str]]] = {}

    @staticmethod
    def _with_retry(fn, max_attempts: int = 6, base_delay: float = 5.0):
        for attempt in range(max_attempts):
            try:
                return fn()
            except gspread.exceptions.APIError as e:
                status = getattr(e.response, "status_code", None) if hasattr(e, "response") else None
                msg = str(e)
                if status == 429 or "429" in msg or "Quota exceeded" in msg:
                    delay = base_delay * (2 ** attempt)
                    logger.warning("sheets 429, retrying in %.0fs (attempt %d/%d)", delay, attempt + 1, max_attempts)
                    time.sleep(delay)
                    continue
                raise
        raise RuntimeError("exhausted retries on Sheets API")

    def values(self, tab: str) -> list[list[str]]:
        if tab in self._cache:
            return self._cache[tab]
        ws = self._with_retry(lambda: self.sh.worksheet(tab))
        rows = self._with_retry(lambda: ws.get_all_values())
        self._cache[tab] = rows
        logger.info("cached tab %r (%d rows)", tab, len(rows))
        # Light pacing to stay under 60 reads/min
        time.sleep(1.2)
        return rows


# ---------- tab parsers ----------

def parse_types(cache: SheetCache) -> list[dict]:
    out = []
    for r in cache.values("Type"):
        code = _clean(r[0] if len(r) > 0 else "")
        if not code:
            continue
        out.append({
            "type_code": code,
            "material": _clean(r[1] if len(r) > 1 else ""),
            "description_th": _clean(r[2] if len(r) > 2 else ""),
        })
    logger.info("parsed %d fastener types", len(out))
    return out


def parse_threads(cache: SheetCache) -> list[dict]:
    out = []
    for r in cache.values("Thread"):
        thread = _clean(r[0] if len(r) > 0 else "")
        if not thread:
            continue
        pitch = _to_float(r[1] if len(r) > 1 else None)
        out.append({"thread": thread, "pitch_mm": pitch})
    logger.info("parsed %d thread entries", len(out))
    return out


def parse_config(cache: SheetCache) -> list[dict]:
    rows = cache.values("Config")
    out = []
    current_section = ""
    for r in rows[3:]:
        # Section headers sit in col 3 with cols 0-2 empty
        size = _clean(r[0] if len(r) > 0 else "")
        bolt_type = _clean(r[1] if len(r) > 1 else "")
        type_and_size = _clean(r[2] if len(r) > 2 else "")
        section_label = _clean(r[3] if len(r) > 3 else "")
        if section_label and not size and not bolt_type:
            current_section = section_label
            continue
        if not size and not bolt_type:
            continue
        out.append({
            "section": current_section,
            "size_mm": size,
            "bolt_type": bolt_type,
            "type_and_size": type_and_size,
            "design_pattern": section_label,
            "nut_size": _clean(r[4] if len(r) > 4 else ""),
            "nut_qty": _to_int(r[5] if len(r) > 5 else None),
            "spring_thickness": _clean(r[6] if len(r) > 6 else ""),
            "spring_qty": _to_int(r[7] if len(r) > 7 else None),
            "washer_thickness": _clean(r[8] if len(r) > 8 else ""),
            "washer_qty": _to_int(r[9] if len(r) > 9 else None),
            "non_shrink": _clean(r[10] if len(r) > 10 else ""),
            "plates": _clean(r[11] if len(r) > 11 else ""),
            "embed_depth": _clean(r[12] if len(r) > 12 else ""),
            "thickness_1": _clean(r[13] if len(r) > 13 else ""),
            "thickness_2": _clean(r[14] if len(r) > 14 else ""),
            "bolt_head": _clean(r[15] if len(r) > 15 else ""),
            "thread_pitch": _clean(r[16] if len(r) > 16 else ""),
            "wrench": _clean(r[17] if len(r) > 17 else ""),
            "washer_id": _clean(r[18] if len(r) > 18 else ""),
            "washer_od": _clean(r[19] if len(r) > 19 else ""),
        })
    logger.info("parsed %d config rows", len(out))
    return out


def parse_fittings(cache: SheetCache) -> list[dict]:
    rows = cache.values("Fittings")
    out = []
    current_section = ""
    for r in rows[3:]:
        size = _clean(r[0] if len(r) > 0 else "")
        model = _clean(r[1] if len(r) > 1 else "")
        project = _clean(r[2] if len(r) > 2 else "")
        if project and not size and not model:
            current_section = project
            continue
        if not size and not model:
            continue
        out.append({
            "section": current_section,
            "size_mm": size,
            "model": model,
            "project": project,
            "length_mm": _clean(r[3] if len(r) > 3 else ""),
            "qty_per_set": _to_int(r[4] if len(r) > 4 else None),
            "stock": _to_int(r[5] if len(r) > 5 else None),
            "order_actual": _to_int(r[6] if len(r) > 6 else None),
            "extra_qty": _to_float(r[7] if len(r) > 7 else None),
            "type_and_size": _clean(r[8] if len(r) > 8 else ""),
            "full_label": _clean(r[9] if len(r) > 9 else ""),
            "spacing": _clean(r[10] if len(r) > 10 else ""),
            "keder_length": _clean(r[11] if len(r) > 11 else ""),
        })
    logger.info("parsed %d fitting rows", len(out))
    return out


def parse_pricelist(cache: SheetCache) -> list[dict]:
    rows = cache.values("Pricelist")
    out = []
    for r in rows[1:]:
        model = _clean(r[0] if len(r) > 0 else "")
        desc = _clean(r[1] if len(r) > 1 else "")
        if not model and not desc:
            continue
        out.append({
            "model": model,
            "description": desc,
            "code": _clean(r[2] if len(r) > 2 else ""),
            "unit_cost_thb": _to_float(r[3] if len(r) > 3 else None),
            "qty_per_set": _clean(r[4] if len(r) > 4 else ""),
            "set_amount_thb": _to_float(r[5] if len(r) > 5 else None),
            "total_cost_thb": _to_float(r[6] if len(r) > 6 else None),
        })
    logger.info("parsed %d pricelist rows", len(out))
    return out


def parse_tpc_pricelist(cache: SheetCache) -> dict:
    """TPC Bolt M 304 is a pivot: lengths (rows) × thread sizes (cols).

    Returns a vendor pricelist dict suitable for Firestore.
    """
    rows = cache.values("TPC Bolt M 304")
    if len(rows) < 6:
        return {"vendor": "TPC", "product": "Bolt M 304", "prices": []}

    # Row index 3 has thread sizes (2mm, 2.5mm, ..., 20mm) in even columns starting at col 2
    sizes_row = rows[3]
    pitches_row = rows[4]
    # Collect size columns as (col_index, size_label, pitch_label)
    size_cols: list[tuple[int, str, str]] = []
    for i in range(2, len(sizes_row), 2):
        size_label = _clean(sizes_row[i])
        pitch_label = _clean(pitches_row[i] if i < len(pitches_row) else "")
        if size_label:
            size_cols.append((i, size_label, pitch_label))

    # Data rows alternate: price row + pack row
    prices = []
    i = 5
    while i < len(rows):
        length_label = _clean(rows[i][0] if rows[i] else "")
        if not length_label:
            i += 1
            continue
        pack_row = rows[i + 1] if i + 1 < len(rows) else []
        for col, size_label, pitch_label in size_cols:
            price_cell = _clean(rows[i][col] if col < len(rows[i]) else "")
            pack_cell = _clean(pack_row[col] if col < len(pack_row) else "")
            price = _to_float(price_cell)
            if price is not None and price > 0:
                prices.append({
                    "length": length_label,
                    "size": size_label,
                    "pitch": pitch_label,
                    "unit_price_thb": price,
                    "pack": pack_cell,
                })
        i += 2

    logger.info("parsed %d TPC pivot prices", len(prices))
    return {
        "vendor": "TPC",
        "product": "Bolt M 304 Stainless",
        "currency": "THB",
        "description": "สกรูหัวหกเหลี่ยมมิลสเตนเลส 304",
        "prices": prices,
    }


def parse_abpon_pricelist(cache: SheetCache) -> dict:
    """Abpon pricelist — flat with thread, length, list price, discount."""
    rows = cache.values("Abpon")
    out = []
    for r in rows[5:]:
        item_th = _clean(r[0] if len(r) > 0 else "")
        material = _clean(r[1] if len(r) > 1 else "")
        if not item_th and not material:
            continue
        detail = _clean(r[2] if len(r) > 2 else "")
        out.append({
            "item_th": item_th,
            "material": material,
            "detail": detail,
            "thread_mm": _clean(r[3] if len(r) > 3 else ""),
            "thread_inch": _clean(r[4] if len(r) > 4 else ""),
            "length_mm": _clean(r[5] if len(r) > 5 else ""),
            "length_inch": _clean(r[6] if len(r) > 6 else ""),
            "od_mm": _clean(r[7] if len(r) > 7 else ""),
            "thickness_mm": _clean(r[8] if len(r) > 8 else ""),
            "thread_thickness_mm": _clean(r[9] if len(r) > 9 else ""),
            "remarks": _clean(r[10] if len(r) > 10 else ""),
            "list_price_thb": _to_float(r[11] if len(r) > 11 else None),
            "discount_pct": _to_float(r[12] if len(r) > 12 else None),
            "net_price_thb": _to_float(r[13] if len(r) > 13 else None),
            "unit": _clean(r[14] if len(r) > 14 else ""),
        })
    logger.info("parsed %d Abpon price rows", len(out))
    return {
        "vendor": "Abpon",
        "product": "Stainless Fasteners",
        "currency": "THB",
        "prices": out,
    }


def parse_fasteners_master(cache: SheetCache) -> tuple[list[dict], list[dict], list[dict]]:
    """Parse the 'Fasteners' master tab.

    Returns (fasteners, order_metadata, order_rows).
    - fasteners: catalog rows (one per model)
    - order_metadata: order column headers (project + date per Order N)
    - order_rows: per-order line items with qty > 0 (subcollection entries)
    """
    rows = cache.values("Fasteners")
    if len(rows) < 5:
        raise ValueError("Fasteners tab unexpectedly small")

    header = rows[0]
    projects = rows[1]
    dates = rows[2]

    # Detect the non-order tail columns (L1, Pitch, Thickness, Pack, Price Date, List, Discount, Cost)
    tail_labels = {"L1", "Pitch", "Thickness", "Pack", "Price Date", "List", "Discount", "Cost"}
    order_cols: list[int] = []
    tail_cols: dict[str, int] = {}
    for idx, h in enumerate(header):
        if h in tail_labels:
            tail_cols[h] = idx
        elif h.startswith("Order"):
            order_cols.append(idx)

    # Order metadata docs
    order_metadata = []
    for col in order_cols:
        label = header[col] if col < len(header) else ""
        project = projects[col] if col < len(projects) else ""
        date = dates[col] if col < len(dates) else ""
        order_metadata.append({
            "order_label": _clean(label),
            "column_index": col,
            "project": _clean(project),
            "date_raw": _clean(date),
        })

    fasteners: list[dict] = []
    order_rows: list[dict] = []

    for ri, r in enumerate(rows[5:], start=5):
        # Ensure length
        if len(r) < len(header):
            r = r + [""] * (len(header) - len(r))
        type_code = _clean(r[0])
        model = _clean(r[1])
        if not model:
            continue
        thread = _clean(r[3])
        description = _clean(r[4])

        length_text = _clean(r[tail_cols["L1"]]) if "L1" in tail_cols else ""
        pitch = _to_float(r[tail_cols["Pitch"]]) if "Pitch" in tail_cols else None
        thickness = _clean(r[tail_cols["Thickness"]]) if "Thickness" in tail_cols else ""
        pack = _clean(r[tail_cols["Pack"]]) if "Pack" in tail_cols else ""
        price_date = _clean(r[tail_cols["Price Date"]]) if "Price Date" in tail_cols else ""
        list_price = _to_float(r[tail_cols["List"]]) if "List" in tail_cols else None
        discount_pct = _to_float(r[tail_cols["Discount"]]) if "Discount" in tail_cols else None
        cost = _to_float(r[tail_cols["Cost"]]) if "Cost" in tail_cols else None

        # Parse length from tail or model
        length_mm = _to_float(length_text.replace("mm", "").strip()) if length_text else None
        if length_mm is None:
            length_mm = _parse_length_from_model(model)

        # Orders: qty > 0
        order_qty_total = 0.0
        orders_count = 0
        for col in order_cols:
            qty = _to_float(r[col] if col < len(r) else None)
            if qty and qty != 0:
                order_qty_total += qty
                orders_count += 1
                order_rows.append({
                    "parent_model": model,
                    "order_label": _clean(header[col]),
                    "column_index": col,
                    "project": _clean(projects[col] if col < len(projects) else ""),
                    "date_raw": _clean(dates[col] if col < len(dates) else ""),
                    "qty": qty,
                })

        fasteners.append({
            "model": model,
            "type_code": type_code,
            "thread": thread,
            "thread_pitch_mm": pitch,
            "length_mm": length_mm,
            "length_text": length_text,
            "thickness": thickness,
            "pack_text": pack,
            "description": description,
            "source_row": ri,
            "last_price_date": price_date,
            "list_price_thb": list_price,
            "discount_pct": discount_pct,
            "cost_thb": cost,
            "total_ordered": order_qty_total,
            "order_count": orders_count,
        })

    logger.info(
        "parsed Fasteners master: %d fasteners, %d orders-metadata, %d order line items",
        len(fasteners), len(order_metadata), len(order_rows),
    )
    return fasteners, order_metadata, order_rows


def parse_total_order(cache: SheetCache) -> list[dict]:
    rows = cache.values("Total Order")
    out = []
    current_section = ""
    for r in rows[3:]:
        size = _clean(r[0] if len(r) > 0 else "")
        bolt_type = _clean(r[1] if len(r) > 1 else "")
        section = _clean(r[3] if len(r) > 3 else "")
        if section and not size:
            current_section = section
            continue
        if not size and not bolt_type:
            continue
        out.append({
            "section": current_section,
            "size": size,
            "bolt_type": bolt_type,
            "description": _clean(r[3] if len(r) > 3 else ""),
            "length": _clean(r[4] if len(r) > 4 else ""),
            "pitch": _clean(r[5] if len(r) > 5 else ""),
            "thickness": _clean(r[6] if len(r) > 6 else ""),
            "thread_type": _clean(r[7] if len(r) > 7 else ""),
            "thread_length": _clean(r[8] if len(r) > 8 else ""),
            "required": _to_float(r[10] if len(r) > 10 else None),
            "extra_order": _to_float(r[11] if len(r) > 11 else None),
            "stock": _to_float(r[12] if len(r) > 12 else None),
            "actual_order": _to_float(r[13] if len(r) > 13 else None),
            "type_and_size": _clean(r[14] if len(r) > 14 else ""),
            "full_label": _clean(r[15] if len(r) > 15 else ""),
        })
    logger.info("parsed %d Total Order rows", len(out))
    return out


def parse_so_sheet(cache: SheetCache, tab: str) -> dict:
    """Parse a purchase order tab (SO21-018, SO23-014 304, etc.) as a document."""
    rows = cache.values(tab)
    if not rows:
        return {"po_id": tab, "title": "", "items": []}
    title = _clean(rows[0][2] if len(rows[0]) > 2 else "")
    subtitle = _clean(rows[1][2] if len(rows) > 1 and len(rows[1]) > 2 else "")
    items = []
    for r in rows[3:]:
        # Skip fully empty rows or pure section headers
        first = _clean(r[0] if len(r) > 0 else "")
        model = _clean(r[1] if len(r) > 1 else "")
        desc = _clean(r[2] if len(r) > 2 else "")
        if not any(_clean(c) for c in r):
            continue
        items.append({
            "col_0": first,
            "col_1": model,
            "col_2": desc,
            "values": [_clean(c) for c in r[:20]],
        })
    return {
        "po_id": tab,
        "title": title,
        "subtitle": subtitle,
        "item_count": len(items),
        "items": items,
    }


# ---------- writers ----------

def write_reference(
    db: firestore.Client,
    collection: str,
    docs: list[dict],
    doc_id_fn,
) -> int:
    coll = db.collection(collection)
    ops = []
    seen_ids: set[str] = set()
    for d in docs:
        did = doc_id_fn(d)
        # De-dupe within a single load
        suffix = 1
        base = did
        while did in seen_ids:
            suffix += 1
            did = f"{base}-{suffix}"
        seen_ids.add(did)
        ops.append((coll.document(did), d))
    return _batch_commit(db, ops)


def write_fasteners(
    db: firestore.Client,
    fasteners: list[dict],
    order_rows: list[dict],
) -> tuple[int, int]:
    """Write master fastener docs + orders subcollection."""
    coll = db.collection("fasteners")

    # Main doc upserts
    main_ops = [(coll.document(_slug(f["model"])), f) for f in fasteners]
    fasteners_written = _batch_commit(db, main_ops)

    # Group order rows by parent model
    by_model: dict[str, list[dict]] = defaultdict(list)
    for row in order_rows:
        by_model[row["parent_model"]].append(row)

    # Write subcollection
    sub_ops = []
    for model, orders in by_model.items():
        parent_ref = coll.document(_slug(model))
        for o in orders:
            # doc id = slugified order label + column index to keep it unique
            sub_id = f"{_slug(o['order_label'])}-c{o['column_index']}"
            sub_ops.append((parent_ref.collection("orders").document(sub_id), o))
    orders_written = _batch_commit(db, sub_ops)
    return fasteners_written, orders_written


def record_sync_run(db: firestore.Client, payload: dict) -> str:
    _, ref = db.collection("fastener_sync_runs").add(payload)
    return ref.id


# ---------- orchestration ----------

def run(dry_run: bool = False) -> dict:
    gc, db = get_clients()
    cache = SheetCache(gc, SPREADSHEET_ID)

    logger.info("parsing source spreadsheet tabs...")
    types_data = parse_types(cache)
    threads_data = parse_threads(cache)
    config_data = parse_config(cache)
    fittings_data = parse_fittings(cache)
    pricelist_data = parse_pricelist(cache)
    tpc_pricelist = parse_tpc_pricelist(cache)
    abpon_pricelist = parse_abpon_pricelist(cache)
    fasteners, order_metadata, order_rows = parse_fasteners_master(cache)
    total_order_data = parse_total_order(cache)

    po_tabs = [
        "SO21-018", "SO21-011", "SO21-038", "SO21-034",
        "SO23-014 304", "SO23-014 A325",
        "SO23-038 Whizdom Kid's Gym",
        "SO22-043 RC International School",
        "SO23-032A Singha DH2 Playground",
    ]
    purchase_orders = []
    for tab in po_tabs:
        try:
            purchase_orders.append(parse_so_sheet(cache, tab))
        except gspread.exceptions.WorksheetNotFound:
            logger.warning("PO tab not found: %s", tab)

    summary = {
        "types": len(types_data),
        "threads": len(threads_data),
        "config_rows": len(config_data),
        "fittings_rows": len(fittings_data),
        "pricelist_rows": len(pricelist_data),
        "tpc_prices": len(tpc_pricelist["prices"]),
        "abpon_prices": len(abpon_pricelist["prices"]),
        "fasteners": len(fasteners),
        "order_metadata": len(order_metadata),
        "order_line_items": len(order_rows),
        "total_order_rows": len(total_order_data),
        "purchase_orders": len(purchase_orders),
    }
    logger.info("parse summary: %s", summary)

    if dry_run:
        logger.info("dry-run: skipping writes")
        return {"dry_run": True, "summary": summary}

    # -------- writes --------
    logger.info("writing to Firestore...")

    # Reference: types
    write_reference(
        db, "fastener_types", types_data,
        lambda d: _slug(d["type_code"]),
    )
    # Reference: threads
    write_reference(
        db, "fastener_threads", threads_data,
        lambda d: _slug(d["thread"]),
    )
    # Config
    write_reference(
        db, "fastener_config", config_data,
        lambda d: _slug(f"{d['section']}-{d['type_and_size'] or d['bolt_type']}-{d['size_mm']}") or "row",
    )
    # Fittings
    write_reference(
        db, "fastener_fittings", fittings_data,
        lambda d: _slug(f"{d['section']}-{d['full_label'] or d['model']}-{d['size_mm']}-{d['length_mm']}"),
    )
    # Pricelist
    write_reference(
        db, "fastener_pricelist", pricelist_data,
        lambda d: _slug(d["model"] or d["description"]),
    )
    # Vendor pricelists (1 doc per vendor product)
    vendor_ops = [
        (db.collection("fastener_vendor_pricelists").document(_slug(f"{p['vendor']}-{p['product']}")), p)
        for p in [tpc_pricelist, abpon_pricelist]
    ]
    _batch_commit(db, vendor_ops)
    # Order metadata (one doc per Order N column)
    write_reference(
        db, "fastener_orders", order_metadata,
        lambda d: _slug(f"{d['order_label']}-c{d['column_index']}"),
    )
    # Total Order rollup
    write_reference(
        db, "fastener_total_orders", total_order_data,
        lambda d: _slug(f"{d['section']}-{d['full_label'] or d['description']}-{d['length']}"),
    )
    # Purchase orders
    write_reference(
        db, "fastener_purchase_orders", purchase_orders,
        lambda d: _slug(d["po_id"]),
    )
    # Fasteners + orders subcollection
    fasteners_written, orders_written = write_fasteners(db, fasteners, order_rows)

    now = datetime.now(timezone.utc)
    run_doc = {
        "run_at": now,
        "source_spreadsheet_id": SPREADSHEET_ID,
        "source_spreadsheet_title": "Fasteners Schedule Ver. 2024",
        "counts": {
            **summary,
            "fasteners_written": fasteners_written,
            "order_subcollection_docs": orders_written,
        },
        "status": "success",
    }
    run_id = record_sync_run(db, run_doc)
    logger.info("sync run recorded: %s", run_id)
    return {"dry_run": False, "summary": summary, "run_id": run_id}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Parse + log counts, do not write to Firestore")
    args = parser.parse_args()
    try:
        result = run(dry_run=args.dry_run)
        print("\n=== RESULT ===")
        for k, v in result.items():
            print(f"{k}: {v}")
        return 0
    except Exception as exc:  # noqa: BLE001
        logger.exception("load failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
