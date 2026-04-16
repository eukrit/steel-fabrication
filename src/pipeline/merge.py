"""Merge standards data, existing sheet data, and scraped prices."""
import logging
from datetime import datetime, timezone

from config.constants import INCH_TO_DN, INCH_TO_OD
from src.scraper.models import ScrapedProduct
from src.sheets.models import ExistingSheetRow, OutputSheetRow
from src.standards.models import SteelSection

logger = logging.getLogger(__name__)


def _make_key(od_mm: float, t_mm: float) -> str:
    """Create a lookup key from OD and thickness."""
    return f"{od_mm:.1f}_{t_mm:.1f}"


def merge_all_sources(
    standards: list[SteelSection],
    existing: list[ExistingSheetRow],
    scraped: list[ScrapedProduct],
) -> list[OutputSheetRow]:
    """Merge all data sources into a sorted list of OutputSheetRows.

    Priority:
    1. Standards data (TIS 107 + JIS G3444) for section properties
    2. Scraped OneStockHome data for prices and URLs
    3. Existing sheet data as price fallback
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Build scraped price lookup: (od_mm, thickness_mm) → ScrapedProduct
    scraped_lookup: dict[str, ScrapedProduct] = {}
    for sp in scraped:
        if sp.outside_diameter_mm and sp.thickness_mm and sp.price_thb:
            key = _make_key(sp.outside_diameter_mm, sp.thickness_mm)
            # Keep the cheapest price if multiple matches
            if key not in scraped_lookup or (sp.price_thb < (scraped_lookup[key].price_thb or float("inf"))):
                scraped_lookup[key] = sp

    # Build existing sheet price lookup: (od_mm, thickness_mm) → ExistingSheetRow
    existing_lookup: dict[str, ExistingSheetRow] = {}
    for er in existing:
        if er.diameter_mm and er.thickness_mm and er.price_thb:
            key = _make_key(er.diameter_mm, er.thickness_mm)
            existing_lookup[key] = er

    # Track which (OD, thickness) combos we've seen to handle TIS+JIS overlap
    seen: dict[str, OutputSheetRow] = {}

    for sec in standards:
        key = _make_key(sec.outside_diameter_mm, sec.thickness_mm)

        # If we already have this size from another standard, mark as BOTH
        if key in seen:
            if seen[key].standard != sec.standard:
                seen[key].standard = "BOTH"
            continue

        # Look up pricing
        price_thb = None
        osh_url = None
        scraped_product = scraped_lookup.get(key)
        existing_row = existing_lookup.get(key)

        if scraped_product and scraped_product.price_thb:
            price_thb = scraped_product.price_thb
            osh_url = scraped_product.url
        elif existing_row and existing_row.price_thb:
            price_thb = existing_row.price_thb

        # Compute derived prices
        price_per_meter = round(price_thb / 6.0, 2) if price_thb else None
        price_per_kg = (
            round(price_per_meter / sec.weight_kg_per_m, 2)
            if price_per_meter and sec.weight_kg_per_m
            else None
        )

        row = OutputSheetRow(
            nominal_size_inch=sec.nominal_size_inch,
            dn=sec.dn,
            outside_diameter_mm=sec.outside_diameter_mm,
            thickness_mm=sec.thickness_mm,
            standard=sec.standard,
            grade=sec.grade,
            weight_kg_per_m=sec.weight_kg_per_m,
            cross_section_area_cm2=sec.cross_section_area_cm2,
            moment_of_inertia_cm4=sec.moment_of_inertia_cm4,
            section_modulus_cm3=sec.section_modulus_cm3,
            radius_of_gyration_cm=sec.radius_of_gyration_cm,
            osh_url=osh_url,
            price_thb=price_thb,
            price_per_meter=price_per_meter,
            price_per_kg=price_per_kg,
            last_updated=now,
        )
        seen[key] = row

    # Sort by OD ascending, then thickness ascending
    results = sorted(
        seen.values(),
        key=lambda r: (r.outside_diameter_mm, r.thickness_mm),
    )

    logger.info(
        f"Merged {len(results)} sections "
        f"({len(scraped_lookup)} scraped prices, "
        f"{len(existing_lookup)} existing prices)"
    )
    return results
