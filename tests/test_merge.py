"""Tests for the merge pipeline."""
from src.pipeline.merge import merge_all_sources
from src.scraper.models import ScrapedProduct
from src.sheets.models import ExistingSheetRow
from src.standards.jis_g3444 import get_jis_g3444_sections
from src.standards.tis107 import get_tis107_sections


def test_merge_standards_only():
    """Merge with standards data only, no prices."""
    tis = get_tis107_sections()
    jis = get_jis_g3444_sections()
    merged = merge_all_sources(tis + jis, [], [])
    # Should have unique (OD, thickness) combinations
    assert len(merged) > 0
    # Should be sorted by OD
    ods = [r.outside_diameter_mm for r in merged]
    assert ods == sorted(ods)
    # All should have no price
    for r in merged:
        assert r.price_thb is None


def test_merge_with_scraped_price():
    """Scraped prices should be matched by OD + thickness."""
    tis = get_tis107_sections()
    scraped = [
        ScrapedProduct(
            product_name="ท่อเหล็กกลม 1 นิ้ว",
            outside_diameter_mm=34.0,
            thickness_mm=2.3,
            price_thb=300.0,
            url="https://example.com/pipe-1",
        )
    ]
    merged = merge_all_sources(tis, [], scraped)
    # Find the 1" x 2.3 entry
    match = [r for r in merged if r.outside_diameter_mm == 34.0 and r.thickness_mm == 2.3]
    assert len(match) == 1
    assert match[0].price_thb == 300.0
    assert match[0].price_per_meter == 50.0  # 300 / 6
    assert match[0].osh_url == "https://example.com/pipe-1"


def test_merge_overlap_marked_as_both():
    """Sections in both TIS and JIS should be marked as 'BOTH'."""
    tis = get_tis107_sections()
    jis = get_jis_g3444_sections()
    merged = merge_all_sources(tis + jis, [], [])
    both = [r for r in merged if r.standard == "BOTH"]
    assert len(both) > 5  # Many sizes overlap


def test_merge_existing_sheet_fallback():
    """When no scraped price, fall back to existing sheet data."""
    tis = get_tis107_sections()
    existing = [
        ExistingSheetRow(
            model='3/4" 27.2 x 2.3',
            diameter_mm=27.2,
            thickness_mm=2.3,
            price_thb=325.0,
        )
    ]
    merged = merge_all_sources(tis, existing, [])
    match = [r for r in merged if r.outside_diameter_mm == 27.2 and r.thickness_mm == 2.3]
    assert len(match) == 1
    assert match[0].price_thb == 325.0
