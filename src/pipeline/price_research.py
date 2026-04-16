"""Automated steel price benchmarking — compares our CHS prices against market references.

Usage:
    python -m src.pipeline.price_research          # print to console
    python -m src.pipeline.price_research --sheet   # also write to Google Sheet tab 'Price Research'

Sources:
    1. Our vendor_prices (Firestore)
    2. SteelLead Thailand (local retail reference)
    3. Trading Economics (global HRC + China rebar)
    4. BOT exchange rate (THB/USD)
"""
import json
import logging
import re
from datetime import datetime, timezone

import httpx
from bs4 import BeautifulSoup

from config.settings import settings

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "th,en;q=0.9",
}


# ---------------------------------------------------------------------------
# Source 1: Our own prices from Firestore
# ---------------------------------------------------------------------------
def fetch_our_prices() -> list[dict]:
    """Read vendor_prices from Firestore and compute ฿/kg stats."""
    from google.cloud import firestore

    db = firestore.Client(
        project=settings.gcp_project_id,
        database=settings.firestore_database,
    )
    docs = db.collection("vendor_prices").stream()

    prices = []
    for doc in docs:
        d = doc.to_dict()
        price_kg = d.get("price_per_kg")
        if price_kg and price_kg > 0:
            prices.append(
                {
                    "section_id": d.get("section_id", ""),
                    "vendor": d.get("vendor_id", ""),
                    "price_thb": d.get("price_thb", 0),
                    "price_per_m": d.get("price_per_meter", 0),
                    "price_per_kg": price_kg,
                }
            )

    return prices


def analyze_our_prices(prices: list[dict]) -> dict:
    """Compute stats from our price data."""
    if not prices:
        return {"count": 0}

    vals = [p["price_per_kg"] for p in prices]

    # Group by size range (parse from section_id like CHS_8in_6.0_BOTH)
    size_groups: dict[str, list[float]] = {}
    for p in prices:
        match = re.search(r"CHS_(.+?)in_", p["section_id"])
        size = match.group(1).replace("-", "/") + '"' if match else "?"
        size_groups.setdefault(size, []).append(p["price_per_kg"])

    by_size = {}
    for size, group_vals in sorted(
        size_groups.items(), key=lambda x: _inch_sort_key(x[0])
    ):
        by_size[size] = {
            "count": len(group_vals),
            "avg": round(sum(group_vals) / len(group_vals), 2),
            "min": round(min(group_vals), 2),
            "max": round(max(group_vals), 2),
        }

    return {
        "count": len(vals),
        "avg_thb_kg": round(sum(vals) / len(vals), 2),
        "min_thb_kg": round(min(vals), 2),
        "max_thb_kg": round(max(vals), 2),
        "by_size": by_size,
    }


def _inch_sort_key(size_str: str) -> float:
    """Convert inch string like '8"' or '1/2"' to a sortable float."""
    s = size_str.replace('"', "").strip()
    parts = s.split()
    total = 0.0
    for part in parts:
        if "/" in part:
            num, den = part.split("/")
            total += float(num) / float(den)
        else:
            try:
                total += float(part)
            except ValueError:
                pass
    return total


# ---------------------------------------------------------------------------
# Source 2: SteelLead Thailand (local retail)
# ---------------------------------------------------------------------------
def fetch_steellead_prices() -> dict:
    """Scrape SteelLead round pipe price page for Thai retail reference."""
    url = "https://www.steellead.com/round-pipe-price.html"
    try:
        with httpx.Client(timeout=15.0) as client:
            resp = client.get(url, headers=_HEADERS, follow_redirects=True)
            resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find date
        date_text = ""
        for el in soup.find_all(string=re.compile(r"\d{1,2}\s+\w+\s+\d{4}")):
            date_text = el.strip()
            break

        # Extract prices from tables
        prices = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) < 4:
                    continue
                texts = [c.get_text(strip=True) for c in cells]
                # Look for rows with numeric price data
                price_kg = _try_parse_float(texts[-2] if len(texts) >= 2 else "")
                price_piece = _try_parse_float(texts[-1] if len(texts) >= 1 else "")
                if price_kg and price_kg > 10 and price_kg < 100:
                    prices.append(
                        {
                            "description": " ".join(texts[:2]),
                            "price_per_kg": price_kg,
                            "price_per_piece": price_piece,
                        }
                    )

        vals = [p["price_per_kg"] for p in prices if p["price_per_kg"]]
        return {
            "source": "SteelLead Thailand",
            "url": url,
            "date": date_text,
            "count": len(prices),
            "avg_thb_kg": round(sum(vals) / len(vals), 2) if vals else None,
            "min_thb_kg": round(min(vals), 2) if vals else None,
            "max_thb_kg": round(max(vals), 2) if vals else None,
            "prices": prices[:20],
        }
    except Exception as e:
        logger.warning(f"SteelLead scrape failed: {e}")
        return {"source": "SteelLead Thailand", "error": str(e)}


# ---------------------------------------------------------------------------
# Source 3: Trading Economics — global benchmarks
# ---------------------------------------------------------------------------
def fetch_global_benchmarks() -> dict:
    """Fetch HRC steel and China rebar prices from Trading Economics."""
    benchmarks = {}

    with httpx.Client(timeout=15.0) as client:
        # HRC Steel
        try:
            resp = client.get(
                "https://tradingeconomics.com/commodity/hrc-steel",
                headers=_HEADERS,
                follow_redirects=True,
            )
            text = resp.text
            # Parse price from meta or structured data
            match = re.search(
                r"HRC Steel.*?(\d[\d,]*\.?\d*)\s*USD/T", text, re.IGNORECASE
            )
            if match:
                benchmarks["hrc_usd_ton"] = _try_parse_float(match.group(1))
            else:
                # Try og:description or title
                match = re.search(r"(\d[\d,]*\.?\d*)\s*USD/T", text)
                if match:
                    benchmarks["hrc_usd_ton"] = _try_parse_float(match.group(1))
        except Exception as e:
            logger.warning(f"HRC fetch failed: {e}")

        # China Rebar
        try:
            resp = client.get(
                "https://tradingeconomics.com/commodity/steel",
                headers=_HEADERS,
                follow_redirects=True,
            )
            text = resp.text
            match = re.search(r"(\d[\d,]*\.?\d*)\s*CNY/T", text)
            if match:
                benchmarks["rebar_cny_ton"] = _try_parse_float(match.group(1))
        except Exception as e:
            logger.warning(f"Rebar fetch failed: {e}")

    return {
        "source": "Trading Economics",
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        **benchmarks,
    }


# ---------------------------------------------------------------------------
# Source 4: BOT exchange rate
# ---------------------------------------------------------------------------
def fetch_exchange_rate() -> float:
    """Fetch THB/USD rate. Falls back to a reasonable default."""
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(
                "https://open.er-api.com/v6/latest/USD",
                follow_redirects=True,
            )
            data = resp.json()
            return data.get("rates", {}).get("THB", 35.5)
    except Exception:
        return 35.5  # fallback


# ---------------------------------------------------------------------------
# Analysis & Report
# ---------------------------------------------------------------------------
def run_price_research(write_sheet: bool = False) -> dict:
    """Run full price benchmarking analysis.

    Returns a report dict with our prices, local references, and global benchmarks.
    """
    now = datetime.now(timezone.utc)
    logger.info("Starting price research...")

    # Fetch all sources
    our_prices = fetch_our_prices()
    our_stats = analyze_our_prices(our_prices)
    steellead = fetch_steellead_prices()
    global_ref = fetch_global_benchmarks()
    fx_rate = fetch_exchange_rate()

    # Compute conversions
    our_avg = our_stats.get("avg_thb_kg", 0)
    our_usd_ton = round(our_avg / fx_rate * 1000, 0) if our_avg and fx_rate else None
    hrc_thb_kg = (
        round(global_ref.get("hrc_usd_ton", 0) * fx_rate / 1000, 2)
        if global_ref.get("hrc_usd_ton")
        else None
    )

    # CHS premium over HRC
    chs_premium = (
        round((our_avg / hrc_thb_kg - 1) * 100, 1)
        if hrc_thb_kg and our_avg
        else None
    )

    report = {
        "date": now.strftime("%Y-%m-%d"),
        "fx_thb_usd": fx_rate,
        "our_prices": {
            **our_stats,
            "avg_usd_kg": round(our_avg / fx_rate, 2) if our_avg else None,
            "avg_usd_ton": our_usd_ton,
        },
        "local_reference": steellead,
        "global_reference": {
            **global_ref,
            "hrc_thb_kg": hrc_thb_kg,
        },
        "analysis": {
            "chs_premium_over_hrc_pct": chs_premium,
            "verdict": _generate_verdict(
                our_avg, hrc_thb_kg, steellead.get("avg_thb_kg")
            ),
        },
    }

    if write_sheet:
        _write_research_to_sheet(report)

    logger.info(f"Price research complete: avg {our_avg} THB/kg")
    return report


def _generate_verdict(
    our_avg: float | None,
    hrc_thb_kg: float | None,
    local_avg: float | None,
) -> str:
    """Generate a human-readable assessment."""
    lines = []

    if our_avg and hrc_thb_kg:
        ratio = our_avg / hrc_thb_kg
        if ratio < 0.9:
            lines.append(
                f"Our avg ({our_avg:.0f} THB/kg) is BELOW HRC raw material "
                f"({hrc_thb_kg:.0f} THB/kg) — excellent wholesale pricing."
            )
        elif ratio < 1.1:
            lines.append(
                f"Our avg ({our_avg:.0f} THB/kg) is NEAR HRC level "
                f"({hrc_thb_kg:.0f} THB/kg) — competitive pricing."
            )
        else:
            lines.append(
                f"Our avg ({our_avg:.0f} THB/kg) is {(ratio-1)*100:.0f}% ABOVE HRC "
                f"({hrc_thb_kg:.0f} THB/kg) — typical CHS fabrication premium."
            )

    if our_avg and local_avg:
        diff = our_avg - local_avg
        if diff > 5:
            lines.append(
                f"Above Thai retail avg ({local_avg:.0f} THB/kg) by "
                f"{diff:.0f} THB/kg — our stock is heavier-wall structural grade."
            )
        elif diff < -5:
            lines.append(
                f"Below Thai retail avg ({local_avg:.0f} THB/kg) by "
                f"{abs(diff):.0f} THB/kg — good value."
            )
        else:
            lines.append(
                f"In line with Thai retail avg ({local_avg:.0f} THB/kg)."
            )

    return " | ".join(lines) if lines else "Insufficient data for comparison."


def _write_research_to_sheet(report: dict) -> None:
    """Write research results to a 'Price Research' tab in the spreadsheet."""
    from config.constants import SPREADSHEET_ID
    from src.sheets.reader import get_gspread_client

    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(SPREADSHEET_ID)

        sheet_name = "Price Research"
        try:
            ws = sh.worksheet(sheet_name)
            ws.clear()
        except Exception:
            ws = sh.add_worksheet(title=sheet_name, rows=100, cols=10)

        rows = [
            ["Steel CHS Price Research", "", "", report["date"]],
            [],
            ["=== OUR PRICES (Firestore vendor_prices) ==="],
            [
                "Metric",
                "THB/kg",
                "USD/kg",
                "USD/ton",
            ],
            [
                "Average",
                report["our_prices"].get("avg_thb_kg"),
                report["our_prices"].get("avg_usd_kg"),
                report["our_prices"].get("avg_usd_ton"),
            ],
            [
                "Min",
                report["our_prices"].get("min_thb_kg"),
            ],
            [
                "Max",
                report["our_prices"].get("max_thb_kg"),
            ],
            [
                "Sections with price",
                report["our_prices"].get("count"),
            ],
            [],
            ["--- By Size ---", "Count", "Avg THB/kg", "Min", "Max"],
        ]

        by_size = report["our_prices"].get("by_size", {})
        for size, stats in by_size.items():
            rows.append(
                [size, stats["count"], stats["avg"], stats["min"], stats["max"]]
            )

        rows.extend(
            [
                [],
                ["=== LOCAL REFERENCE (SteelLead Thailand) ==="],
                [
                    "Date",
                    report["local_reference"].get("date", "N/A"),
                ],
                [
                    "Avg THB/kg",
                    report["local_reference"].get("avg_thb_kg"),
                ],
                [
                    "Range",
                    report["local_reference"].get("min_thb_kg"),
                    "to",
                    report["local_reference"].get("max_thb_kg"),
                ],
                [
                    "Products",
                    report["local_reference"].get("count"),
                ],
                [],
                ["=== GLOBAL REFERENCE (Trading Economics) ==="],
                [
                    "HRC Steel (USD/ton)",
                    report["global_reference"].get("hrc_usd_ton"),
                ],
                [
                    "HRC Steel (THB/kg)",
                    report["global_reference"].get("hrc_thb_kg"),
                ],
                [
                    "China Rebar (CNY/ton)",
                    report["global_reference"].get("rebar_cny_ton"),
                ],
                [
                    "THB/USD rate",
                    report.get("fx_thb_usd"),
                ],
                [],
                ["=== ANALYSIS ==="],
                [
                    "CHS premium over HRC",
                    f"{report['analysis'].get('chs_premium_over_hrc_pct', 'N/A')}%",
                ],
                ["Verdict", report["analysis"].get("verdict", "")],
            ]
        )

        ws.update(range_name="A1", values=rows)
        ws.format("A1", {"textFormat": {"bold": True, "fontSize": 12}})
        ws.format("A3", {"textFormat": {"bold": True}})
        ws.format("A13", {"textFormat": {"bold": True}})
        ws.format("A20", {"textFormat": {"bold": True}})
        ws.format("A28", {"textFormat": {"bold": True}})

        logger.info(f"Wrote price research to '{sheet_name}' tab")
    except Exception as e:
        logger.warning(f"Failed to write research to sheet: {e}")


def _try_parse_float(val: str) -> float | None:
    if not val:
        return None
    cleaned = str(val).strip().replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def print_report(report: dict) -> None:
    """Print a human-readable report to stdout."""
    print("=" * 70)
    print(f"  STEEL CHS PRICE RESEARCH — {report['date']}")
    print(f"  FX Rate: {report['fx_thb_usd']} THB/USD")
    print("=" * 70)

    our = report["our_prices"]
    print(f"\n  OUR PRICES ({our['count']} sections with pricing)")
    print(f"  Average: {our['avg_thb_kg']} THB/kg = ${our.get('avg_usd_kg')}/kg = ${our.get('avg_usd_ton')}/ton")
    print(f"  Range:   {our['min_thb_kg']} - {our['max_thb_kg']} THB/kg")

    print("\n  By Size:")
    for size, stats in our.get("by_size", {}).items():
        print(f"    {size:>8s}  n={stats['count']:>2d}  avg={stats['avg']:>6.1f}  range={stats['min']:.1f}-{stats['max']:.1f}")

    local = report.get("local_reference", {})
    if local.get("avg_thb_kg"):
        print(f"\n  LOCAL REF (SteelLead, {local.get('date', '?')})")
        print(f"  Average: {local['avg_thb_kg']} THB/kg")
        print(f"  Range:   {local.get('min_thb_kg')} - {local.get('max_thb_kg')} THB/kg ({local.get('count')} products)")

    gl = report.get("global_reference", {})
    print(f"\n  GLOBAL REF (Trading Economics, {gl.get('date', '?')})")
    if gl.get("hrc_usd_ton"):
        print(f"  HRC Steel:    ${gl['hrc_usd_ton']}/ton = {gl.get('hrc_thb_kg')} THB/kg")
    if gl.get("rebar_cny_ton"):
        print(f"  China Rebar:  {gl['rebar_cny_ton']} CNY/ton")

    analysis = report.get("analysis", {})
    if analysis.get("chs_premium_over_hrc_pct") is not None:
        print(f"\n  CHS premium over HRC: {analysis['chs_premium_over_hrc_pct']}%")
    print(f"\n  VERDICT: {analysis.get('verdict', 'N/A')}")
    print("=" * 70)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    write_sheet = "--sheet" in sys.argv
    report = run_price_research(write_sheet=write_sheet)
    print_report(report)
