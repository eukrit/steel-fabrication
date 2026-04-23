"""Ingest the Sanko Fastem Thailand file archive into Firestore.

Source folder:
    C:/Users/Eukrit/My Drive/Products GO/Fastener Products/Sanko Fastem Thailand

Walks every file in the folder tree and records a doc per file in
`sanko_documents` (metadata), then parses any PDF we know the shape of:
- Quotation/PO-style PDFs ("QTP*", "ใบเสนอราคา*", "PO-*", "PI-*") → line items
- Structured pricelist PDFs (Drop-in Anchor, Drill bit list) → price rows

Derives a unified `sanko_products` catalog from every product code seen
in line items + hand-coded product families from the Bolt Anchors /
Drill Bits / Epoxy Resin / Bender Cutter / Puncher folders.

Collections
-----------
- sanko_documents         (every file — metadata only)
- sanko_products          (derived product catalog)
- sanko_prices            (flat pricelist rows)
- sanko_quotations        (parsed QTP / quotation PDFs with line items)
- sanko_purchase_orders   (PO / PI PDFs with line items)
- sanko_sync_runs         (audit log)
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from google.cloud import firestore
from pypdf import PdfReader

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("sanko")

SANKO_ROOT = Path(
    "C:/Users/Eukrit/My Drive/Products GO/Fastener Products/Sanko Fastem Thailand"
)
GCP_PROJECT_ID = "ai-agents-go"
FIRESTORE_DATABASE = "steel-sections"
VENDOR = "Sanko Fastem (Thailand) Ltd."


# ---------- helpers ----------

def _slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s or "unnamed"


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _credentials_path() -> str:
    env = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if env and Path(env).exists():
        return env
    creds_dir = Path("C:/Users/Eukrit/OneDrive/Documents/Claude Code/Credentials Claude Code")
    # Match any ai-agents-go-*.json (SA key fingerprint rotates)
    candidates = sorted(creds_dir.glob("ai-agents-go-*.json"))
    if candidates:
        chosen = str(candidates[-1]).replace("\\", "/")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = chosen
        logger.info("using SA key: %s", Path(chosen).name)
        return chosen
    raise FileNotFoundError("ai-agents-go SA key not found in Credentials folder")


def _batch_commit(db: firestore.Client, ops: list[tuple[Any, dict]]) -> int:
    written = 0
    batch = db.batch()
    n = 0
    for ref, payload in ops:
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


def _file_hash(path: Path, limit: int = 1024 * 1024) -> str:
    """First-1MB sha1 — enough to dedupe large docs."""
    h = hashlib.sha1()
    try:
        with path.open("rb") as f:
            h.update(f.read(limit))
    except Exception:
        return ""
    return h.hexdigest()


def _extract_text(path: Path) -> str:
    try:
        r = PdfReader(str(path))
        return "\n".join((pg.extract_text() or "") for pg in r.pages)
    except Exception as e:
        logger.warning("pdf read failed %s: %s", path.name, e)
        return ""


def _date_from_filename(name: str) -> str:
    m = re.match(r"(\d{4}-\d{2}-\d{2})", name)
    return m.group(1) if m else ""


# ---------- classification ----------

def classify(path: Path) -> str:
    """Return a simple category tag for the file."""
    n = path.name.lower()
    parent = path.parent.name.lower() if path.parent != SANKO_ROOT else ""
    if path.suffix.lower() == ".mp4":
        return "video"
    if path.suffix.lower() in {".jpg", ".jpeg", ".png"}:
        return "image"
    if path.suffix.lower() == ".html":
        return "web"
    if path.suffix.lower() != ".pdf":
        return "other"
    # PDFs
    if "qtp" in n or "ใบเสนอราคา" in path.name or "quotation" in n:
        return "quotation"
    if "po-" in n or "ใบสั่งซื้อ" in path.name or "purchase order" in n:
        return "purchase_order"
    if "pi-" in n or "ใบแจ้งหนี้" in path.name or "invoice" in n:
        return "invoice"
    if "price list" in n or "ใบราคา" in path.name or "ราคา" in path.name and "qtp" not in n:
        return "pricelist"
    if "bolt anchors" in str(path).lower():
        return "product_catalog"
    if "drill bits" in str(path).lower():
        return "product_catalog"
    if "epoxy resin" in str(path).lower():
        return "product_catalog"
    if "bender cutter" in str(path).lower():
        return "product_catalog"
    if "puncher" in str(path).lower():
        return "product_catalog"
    if "nmp diamond" in str(path).lower():
        return "product_catalog"
    if "bank" in n or "หนังสือรับรอง" in path.name or "การโอนเงิน" in path.name:
        return "admin"
    return "document"


# ---------- parsers ----------

# Known Sanko product families with metadata.
PRODUCT_FAMILIES: dict[str, dict] = {
    "BA": {"category": "anchor", "family": "Bolt Anchor (BA-Type)"},
    "C":  {"category": "anchor", "family": "Hammer Drive Anchor (C-Type)"},
    "CT": {"category": "anchor", "family": "Drop-in Anchor (CT-Type)"},
    "GA": {"category": "anchor", "family": "Grip Anchor (GA-Type)"},
    "GT": {"category": "anchor", "family": "Lip Anchor (GT-Type)"},
    "HAS":{"category": "anchor", "family": "HAS Chemical Anchor Stud"},
    "SC": {"category": "anchor", "family": "SC-Type"},
    "MD": {"category": "anchor", "family": "Drop-in Anchor (MINORI MD)"},
    "PDF":{"category": "anchor", "family": "Drop-in Anchor (PDF-Type)"},
    "HT": {"category": "tool",   "family": "Setting Tool (HT-Type)"},
    "AH": {"category": "tool",   "family": "Anchor Hammer"},
    "ML": {"category": "drill_bit", "family": "SDS Plus 4-Cutter Rotary Drill Bit"},
    "ZHSS":{"category":"drill_bit", "family": "ZHSS Rotary Drill Bit"},
    "VR": {"category": "chemical", "family": "VR Chemical Anchor"},
    "ER": {"category": "chemical", "family": "Epoxy Resin"},
    "EXT":{"category": "tool",    "family": "Dispenser"},
    "DBD":{"category": "machine", "family": "Rebar Bender (DBD)"},
    "DBR":{"category": "machine", "family": "Rebar Bender (DBR)"},
}


def _product_family_for(code: str) -> dict:
    """Look up family metadata by product code prefix."""
    if not code:
        return {}
    up = code.upper()
    # Longest-prefix match on the letter run at start (handle HAS before HT etc.)
    m = re.match(r"^([A-Z]+)", up)
    if not m:
        return {}
    prefix = m.group(1)
    # Try longest prefix first
    for k in sorted(PRODUCT_FAMILIES.keys(), key=len, reverse=True):
        if up.startswith(k):
            return PRODUCT_FAMILIES[k]
    return {}


# Regex to extract QTP line items. Lines look like:
#   "1 SDS Plus 4-Cutter Rotary Drill Bit Ø10.5X160 765.0085.00Pcs.10ML-10516 85.00"
#
# Groups: (line_no) (description) (amount) (unit_price) unit qty (product_code) (discount)
# The quirk: `amount` and `unit_price` are concatenated with no space between them.
#
# Simpler approach: split on whitespace and work backwards from the end.
def parse_quotation(text: str) -> list[dict]:
    """Parse Sanko quotation-style line items. Works on QTP*, PO-*, PI-* PDFs."""
    lines = text.splitlines()
    items: list[dict] = []
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        # Line items start with a number then a space
        m = re.match(r"^(\d+)\s+(.+)$", line)
        if not m:
            continue
        line_no = int(m.group(1))
        rest = m.group(2)
        # Look for a trailing number (discount) then "<CODE> <qty> <unit> <unit_price><amount>"
        # Pattern observed:
        #   <desc> <amount><unit_price>Pcs.<qty><CODE> <discount>
        # E.g. "SDS Plus 4-Cutter Rotary Drill Bit Ø10.5X160 765.0085.00Pcs.10ML-10516 85.00"
        m2 = re.search(
            r"^(?P<desc>.+?)\s+(?P<amount>[\d,]+\.\d{2})(?P<unit_price>[\d,]+\.\d{2})"
            r"(?P<unit>[A-Za-z\.]+)(?P<qty>\d+(?:\.\d+)?)(?P<code>[A-Z][A-Z0-9\-]+)\s+"
            r"(?P<discount>[\d,]+\.\d{2})\s*$",
            rest,
        )
        if not m2:
            # Alternate pattern without a unit (e.g. Rebar Benders line in 2019-04-23 QTP1904013)
            #   "Rebar Benders 94,290.00134,700.00Pcs.1DBD16L 40,410.00"
            # Already matches above. Try other fallback: amount first is glued to desc (no space)
            # or no trailing unit word:
            m3 = re.search(
                r"^(?P<desc>.+?)\s+(?P<amount>[\d,]+\.\d{2})(?P<unit_price>[\d,]+\.\d{2})"
                r"(?P<qty>\d+(?:\.\d+)?)(?P<code>[A-Z][A-Z0-9\-\(\)]+)\s+"
                r"(?P<discount>[\d,]+\.\d{2})\s*$",
                rest,
            )
            if not m3:
                continue
            g = m3.groupdict()
            g["unit"] = ""
        else:
            g = m2.groupdict()
        items.append({
            "line_no": line_no,
            "description": g["desc"].strip(),
            "product_code": g["code"].strip(),
            "qty": _to_float(g["qty"]),
            "unit": g.get("unit", "").strip(".").strip(),
            "unit_price_thb": _to_float(g["unit_price"]),
            "amount_thb": _to_float(g["amount"]),
            "discount_thb": _to_float(g["discount"]),
        })
    return items


def parse_dropin_pricelist(text: str) -> list[dict]:
    """Parse the '2021-03-12 ดรอปอิน (Price List).pdf' drop-in / chemical list."""
    items = []
    # Example line: "MD-2530 5/16" 5.00                      2.30                                ลังละ 1200 ตัว"
    pattern = re.compile(
        r"^\s*(?P<code>[A-Z][A-Z0-9\-]+)\s+"
        r"(?P<size>[0-9]/?[0-9]?(?:\s\d/\d)?\"?\s*)?\s*"
        r"(?P<list>\d+\.\d{2})\s+"
        r"(?P<net>\d+\.\d{2})\s*"
        r"(?P<note>.*)$",
        re.MULTILINE,
    )
    for m in pattern.finditer(text):
        items.append({
            "product_code": m.group("code"),
            "size": (m.group("size") or "").strip(),
            "list_price_thb": _to_float(m.group("list")),
            "net_price_thb": _to_float(m.group("net")),
            "remarks": (m.group("note") or "").strip(),
        })
    return items


def parse_drill_pricelist(text: str) -> list[dict]:
    """Parse the '2021-03-12 ราคาดอกสว่าน (Price List).pdf' drill-bit list.

    Lines are:
      4-CUTTER ROTARY DRILL BIT 6.0X110  73.00                     43.80
    The product code isn't explicit in the list; synthesize ML-NN-LL.
    """
    items = []
    pattern = re.compile(
        r"4-CUTTER ROTARY DRILL BIT\s+(?P<diam>\d+(?:\.\d+)?)X(?P<len>\d+)\s+"
        r"(?P<list>\d+\.\d{2})\s+(?P<net>\d+\.\d{2})",
        re.IGNORECASE,
    )
    for m in pattern.finditer(text):
        diam = m.group("diam")
        length = m.group("len")
        # Synthesize Sanko product code: ML-<diamX10, 3 digits><length, 2 digits>
        # e.g. 6.0 x 110 -> ML-06011 ? Actually from data samples:
        # ML-07016 = 7.0x160, ML-18021 = 18.0x210 → format: <diam*10 as 3 digits><length/10 as 2 digits>
        try:
            d_int = int(float(diam) * 10)
            l_int = int(int(length) / 10)
            code = f"ML-{d_int:03d}{l_int:02d}"
        except Exception:
            code = ""
        items.append({
            "product_code": code,
            "description": f"SDS Plus 4-Cutter Rotary Drill Bit Ø{diam}X{length}",
            "diameter_mm": _to_float(diam),
            "length_mm": _to_float(length),
            "list_price_thb": _to_float(m.group("list")),
            "net_price_thb": _to_float(m.group("net")),
            "discount_pct": 40.0,  # stated on the pricelist: 40%
        })
    return items


def extract_document_date(name: str, text: str) -> str:
    """Pull a document date from the filename prefix or the PDF text."""
    d = _date_from_filename(name)
    if d:
        return d
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    return m.group(0) if m else ""


def extract_doc_no(name: str, text: str) -> str:
    """Return the first identifier we find (QTP..., PO-..., PI-...)."""
    for pat in (r"QTP\d+", r"PO-\d+", r"PI-\d+"):
        m = re.search(pat, name) or re.search(pat, text)
        if m:
            return m.group(0)
    return ""


# ---------- walk + ingest ----------

def walk_files(root: Path) -> list[Path]:
    out = []
    for p in root.rglob("*"):
        if p.is_file() and not p.name.startswith("."):
            if p.name.lower() == "desktop.ini":
                continue
            out.append(p)
    return sorted(out)


def build_document_record(path: Path) -> dict:
    rel = str(path.relative_to(SANKO_ROOT)).replace("\\", "/")
    stat = path.stat()
    return {
        "relative_path": rel,
        "filename": path.name,
        "extension": path.suffix.lower().lstrip("."),
        "size_bytes": stat.st_size,
        "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        "category": classify(path),
        "folder": str(path.parent.relative_to(SANKO_ROOT)).replace("\\", "/") or ".",
        "file_date": _date_from_filename(path.name),
        "sha1_prefix": _file_hash(path),
        "vendor": VENDOR,
    }


def ingest(db: firestore.Client) -> dict:
    logger.info("scanning %s", SANKO_ROOT)
    files = walk_files(SANKO_ROOT)
    logger.info("found %d files", len(files))

    documents: list[dict] = []
    quotations: list[dict] = []
    purchase_orders: list[dict] = []
    prices: list[dict] = []
    products: dict[str, dict] = {}

    for path in files:
        rec = build_document_record(path)
        rec["_id"] = _slug(rec["relative_path"])
        documents.append(rec)

        if rec["extension"] != "pdf":
            continue

        cat = rec["category"]
        name = path.name

        text = ""
        if cat in {"quotation", "purchase_order", "invoice", "pricelist"}:
            text = _extract_text(path)
        # Try pricelist parsers first — they shape the products catalog cleanly.
        if cat == "pricelist":
            if "ดรอปอิน" in name:
                rows = parse_dropin_pricelist(text)
                for r in rows:
                    r["source"] = rec["relative_path"]
                    r["product_family_hint"] = "Drop-in / Chemical"
                    r["pricelist_date"] = rec["file_date"]
                    prices.append(r)
            elif "ดอกสว่าน" in name:
                rows = parse_drill_pricelist(text)
                for r in rows:
                    r["source"] = rec["relative_path"]
                    r["product_family_hint"] = "Drill Bit"
                    r["pricelist_date"] = rec["file_date"]
                    prices.append(r)
            # Keep the raw text snippet for reference
            rec["text_excerpt"] = text[:2000]

        if cat in {"quotation", "purchase_order", "invoice"}:
            items = parse_quotation(text)
            doc_no = extract_doc_no(name, text)
            entry = {
                "doc_id": doc_no or rec["_id"],
                "doc_type": cat,
                "doc_date": rec["file_date"] or extract_document_date(name, text),
                "source_file": rec["relative_path"],
                "item_count": len(items),
                "items": items,
                "text_excerpt": text[:1500],
            }
            if cat == "quotation":
                quotations.append(entry)
            else:
                purchase_orders.append(entry)

            # Seed products from line items
            for it in items:
                code = it.get("product_code")
                if not code:
                    continue
                rec_p = products.setdefault(code, {
                    "product_code": code,
                    "descriptions": [],
                    "unit_prices_thb": [],
                    "sources": [],
                })
                if it.get("description") and it["description"] not in rec_p["descriptions"]:
                    rec_p["descriptions"].append(it["description"])
                if it.get("unit_price_thb") is not None:
                    rec_p["unit_prices_thb"].append(it["unit_price_thb"])
                if rec["relative_path"] not in rec_p["sources"]:
                    rec_p["sources"].append(rec["relative_path"])

    # Merge pricelist rows into products
    for r in prices:
        code = r.get("product_code")
        if not code:
            continue
        p = products.setdefault(code, {
            "product_code": code,
            "descriptions": [],
            "unit_prices_thb": [],
            "sources": [],
        })
        if r.get("description") and r["description"] not in p["descriptions"]:
            p["descriptions"].append(r["description"])
        if r.get("net_price_thb") is not None:
            p.setdefault("pricelist_net_prices_thb", []).append(r["net_price_thb"])
        if r.get("list_price_thb") is not None:
            p.setdefault("pricelist_list_prices_thb", []).append(r["list_price_thb"])
        if r.get("source") and r["source"] not in p["sources"]:
            p["sources"].append(r["source"])

    # Add product family metadata + latest prices to each product
    product_list = []
    for code, p in products.items():
        fam = _product_family_for(code)
        p["family"] = fam.get("family", "")
        p["category"] = fam.get("category", "other")
        up = p.get("unit_prices_thb", [])
        p["latest_unit_price_thb"] = up[-1] if up else None
        p["min_unit_price_thb"] = min(up) if up else None
        p["max_unit_price_thb"] = max(up) if up else None
        p["source_count"] = len(p.get("sources", []))
        p["description"] = p["descriptions"][0] if p.get("descriptions") else ""
        product_list.append(p)
    product_list.sort(key=lambda x: (x["category"], x["product_code"]))

    # Add hand-coded product families as category "catalog" entries for PDFs we couldn't parse
    catalog_families = [
        ("BA-Type",  "anchor", "Bolt Anchor Stud", "Bolt Anchors/ba-type.pdf"),
        ("C-Type",   "anchor", "Hammer Drive Anchor", "Bolt Anchors/c-type.pdf"),
        ("CT-Type",  "anchor", "Drop-in Anchor", "Bolt Anchors/ct-type.pdf"),
        ("GA-Type",  "anchor", "Grip Anchor", "Bolt Anchors/ga-type.pdf"),
        ("GT-Type",  "anchor", "Lip Anchor", "Bolt Anchors/gt-type.pdf"),
        ("HAS-Type", "anchor", "HAS Chemical Stud", "Bolt Anchors/has-type.pdf"),
        ("SC-Type",  "anchor", "SC-Type Anchor", "Bolt Anchors/sc-type.pdf"),
        ("SDS-Plus-4-Cutter", "drill_bit", "SDS Plus 4-Cutter Rotary Drill Bit", "Drill Bits/4_plus SDS Plus.pdf"),
        ("SDS-Max-4-Cutter",  "drill_bit", "SDS Max 4-Cutter Rotary Drill Bit",  "Drill Bits/4_cutters SDS Max.pdf"),
        ("ER-28",  "chemical", "Epoxy Resin 28",  "Epoxy Resin/er-28.pdf"),
        ("ER-40W", "chemical", "Epoxy Resin 40W", "Epoxy Resin/er-40w.pdf"),
        ("VR-30",  "chemical", "VR-30 Chemical Anchor", "2021-03-12 VR-30.pdf"),
        ("DBD",   "machine", "Rebar Bender DBD",    "Bender Cutter/DBD.pdf"),
        ("DBR-32HD", "machine", "Rebar Bender DBR-32HD", "Bender Cutter/dbr-32hd.pdf"),
        ("DBR-32WH", "machine", "Rebar Bender DBR-32WH", "Bender Cutter/dbr-32wh.pdf"),
        ("Rebar-Cutter-Bender", "machine", "Rebar Cutter & Bender", "Bender Cutter/REBAR CUTTER & BENDER.pdf"),
        ("HandyPuncher", "tool", "Handy Puncher", "Puncher/HandyPuncher.pdf"),
    ]
    for code, category, family, src in catalog_families:
        if code in products:
            # Upgrade existing entry with family info
            products[code].setdefault("family", family)
            products[code]["category"] = products[code].get("category") or category
            products[code].setdefault("catalog_pdf", src)
        else:
            product_list.append({
                "product_code": code,
                "family": family,
                "category": category,
                "description": family,
                "descriptions": [family],
                "catalog_pdf": src,
                "sources": [src],
                "source_count": 1,
                "unit_prices_thb": [],
                "latest_unit_price_thb": None,
            })

    logger.info(
        "summary: docs=%d quotations=%d POs+PIs=%d prices=%d products=%d",
        len(documents), len(quotations), len(purchase_orders), len(prices), len(product_list),
    )

    # -------- writes --------
    # Documents
    doc_coll = db.collection("sanko_documents")
    _batch_commit(db, [(doc_coll.document(d["_id"]), {k: v for k, v in d.items() if k != "_id"})
                       for d in documents])

    # Products
    prod_coll = db.collection("sanko_products")
    _batch_commit(db, [(prod_coll.document(_slug(p["product_code"])), p) for p in product_list])

    # Prices (pricelist rows)
    price_coll = db.collection("sanko_prices")
    _batch_commit(db, [
        (price_coll.document(_slug(f"{r.get('product_code') or r.get('description','')}-{r.get('pricelist_date','')}-{i}")), r)
        for i, r in enumerate(prices)
    ])

    # Quotations
    qt_coll = db.collection("sanko_quotations")
    _batch_commit(db, [(qt_coll.document(_slug(q["doc_id"])), q) for q in quotations])

    # Purchase orders / invoices
    po_coll = db.collection("sanko_purchase_orders")
    _batch_commit(db, [(po_coll.document(_slug(p["doc_id"])), p) for p in purchase_orders])

    # Sync run audit
    run = {
        "run_at": datetime.now(timezone.utc),
        "vendor": VENDOR,
        "source_root": str(SANKO_ROOT),
        "counts": {
            "documents": len(documents),
            "products": len(product_list),
            "prices": len(prices),
            "quotations": len(quotations),
            "purchase_orders": len(purchase_orders),
        },
        "status": "success",
    }
    _, ref = db.collection("sanko_sync_runs").add(run)
    logger.info("run recorded: %s", ref.id)

    return {"run_id": ref.id, "counts": run["counts"]}


def main() -> int:
    _credentials_path()
    db = firestore.Client(project=GCP_PROJECT_ID, database=FIRESTORE_DATABASE)
    try:
        result = ingest(db)
        print("\n=== RESULT ===")
        for k, v in result.items():
            print(f"{k}: {v}")
        return 0
    except Exception as e:  # noqa: BLE001
        logger.exception("failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
