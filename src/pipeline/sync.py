"""Orchestrate the full sync pipeline."""
import logging
from datetime import datetime, timezone

from src.firestore.client import (
    ensure_vendor,
    get_firestore_client,
    record_scrape_run,
    upsert_sections,
    upsert_vendor_prices,
)
from src.firestore.models import ScrapeRunDoc, SectionDoc, VendorDoc, VendorPriceDoc
from src.pipeline.merge import merge_all_sources
from src.scraper.onestockhome import scrape_all_pages
from src.sheets.models import OutputSheetRow
from src.sheets.reader import get_gspread_client, read_chs_jis_m, read_chs_table
from src.sheets.writer import write_chs_jis_claude
from src.standards.jis_g3444 import get_jis_g3444_sections
from src.standards.tis107 import get_tis107_sections

logger = logging.getLogger(__name__)


def run_full_sync() -> dict:
    """Run the full sync pipeline:

    1. Scrape OneStockHome prices
    2. Read existing sheet data
    3. Load standards data
    4. Merge all sources
    5. Write to 'CHS JIS Claude' sheet
    6. Upsert to Firestore (sections + vendor_prices)

    Returns a summary dict.
    """
    started = datetime.now(timezone.utc)
    errors: list[str] = []

    # 1. Scrape OneStockHome
    logger.info("Step 1: Scraping OneStockHome...")
    try:
        scraped = scrape_all_pages()
    except Exception as e:
        errors.append(f"Scrape failed: {e}")
        scraped = []

    # 2. Read existing sheet data
    logger.info("Step 2: Reading existing sheet data...")
    try:
        gc = get_gspread_client()
        existing = read_chs_jis_m(gc)
        od_table = read_chs_table(gc)
    except Exception as e:
        errors.append(f"Sheet read failed: {e}")
        gc = None
        existing = []
        od_table = {}

    # 3. Load standards data
    logger.info("Step 3: Loading standards data...")
    tis107 = get_tis107_sections()
    jis_g3444 = get_jis_g3444_sections()
    all_standards = tis107 + jis_g3444

    # 4. Merge
    logger.info("Step 4: Merging all sources...")
    merged = merge_all_sources(all_standards, existing, scraped)

    # 5. Write to Google Sheet
    sheet_rows = 0
    if gc and merged:
        logger.info("Step 5: Writing to CHS JIS Claude sheet...")
        try:
            write_chs_jis_claude(gc, merged)
            sheet_rows = len(merged)
        except Exception as e:
            errors.append(f"Sheet write failed: {e}")

    # 6. Upsert to Firestore
    sections_written = 0
    prices_updated = 0
    prices_changed = 0
    try:
        logger.info("Step 6: Upserting to Firestore...")
        db = get_firestore_client()

        # Ensure OneStockHome vendor exists
        ensure_vendor(
            db,
            VendorDoc(
                name="OneStockHome",
                url="https://www.onestockhome.com",
                vendor_type="online_marketplace",
                scrape_enabled=True,
                last_synced=started,
            ),
        )

        # Upsert sections (engineering data)
        section_docs = [
            SectionDoc(
                nominal_size_inch=r.nominal_size_inch,
                dn=r.dn,
                outside_diameter_mm=r.outside_diameter_mm,
                thickness_mm=r.thickness_mm,
                standard=r.standard,
                grade=r.grade,
                weight_kg_per_m=r.weight_kg_per_m,
                cross_section_area_cm2=r.cross_section_area_cm2,
                moment_of_inertia_cm4=r.moment_of_inertia_cm4,
                section_modulus_cm3=r.section_modulus_cm3,
                radius_of_gyration_cm=r.radius_of_gyration_cm,
            )
            for r in merged
        ]
        sections_written = upsert_sections(db, section_docs)

        # Upsert vendor prices (only for rows with pricing)
        price_docs = []
        for r in merged:
            if r.price_thb:
                sec_doc = SectionDoc(
                    nominal_size_inch=r.nominal_size_inch,
                    dn=r.dn,
                    outside_diameter_mm=r.outside_diameter_mm,
                    thickness_mm=r.thickness_mm,
                    standard=r.standard,
                    grade=r.grade,
                    weight_kg_per_m=r.weight_kg_per_m,
                )
                price_docs.append(
                    VendorPriceDoc(
                        vendor_id="onestockhome",
                        section_id=sec_doc.doc_id(),
                        product_url=r.osh_url,
                        price_thb=r.price_thb,
                        price_per_meter=r.price_per_meter,
                        price_per_kg=r.price_per_kg,
                        in_stock=True,
                        last_scraped=started,
                    )
                )
        if price_docs:
            prices_updated, prices_changed = upsert_vendor_prices(db, price_docs)

        # Record scrape run
        completed = datetime.now(timezone.utc)
        record_scrape_run(
            db,
            ScrapeRunDoc(
                vendor_id="onestockhome",
                started_at=started,
                completed_at=completed,
                products_scraped=len(scraped),
                sections_updated=prices_updated,
                prices_changed=prices_changed,
                errors=errors,
            ),
        )
    except Exception as e:
        errors.append(f"Firestore upsert failed: {e}")

    summary = {
        "status": "ok" if not errors else "partial",
        "standards_loaded": len(all_standards),
        "products_scraped": len(scraped),
        "existing_rows_read": len(existing),
        "merged_sections": len(merged),
        "sheet_rows_written": sheet_rows,
        "firestore_sections": sections_written,
        "firestore_prices": prices_updated,
        "prices_changed": prices_changed,
        "errors": errors,
        "duration_seconds": (datetime.now(timezone.utc) - started).total_seconds(),
    }

    logger.info(f"Sync complete: {summary}")
    return summary
