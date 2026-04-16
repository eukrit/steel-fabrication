"""Tests for standards data modules."""
from src.standards.jis_g3444 import get_jis_g3444_sections
from src.standards.tis107 import get_tis107_sections


def test_tis107_sections_count():
    sections = get_tis107_sections()
    assert len(sections) == 25  # 25 entries from the PDF


def test_tis107_all_have_required_fields():
    for s in get_tis107_sections():
        assert s.standard == "TIS_107"
        assert s.nominal_size_inch
        assert s.dn > 0
        assert s.outside_diameter_mm > 0
        assert s.thickness_mm > 0
        assert s.weight_kg_per_m > 0


def test_tis107_sizes_sorted():
    sections = get_tis107_sections()
    ods = [s.outside_diameter_mm for s in sections]
    # Should be non-decreasing
    for i in range(1, len(ods)):
        assert ods[i] >= ods[i - 1]


def test_jis_g3444_sections_count():
    sections = get_jis_g3444_sections()
    assert len(sections) == 65  # 65 entries from the PDF


def test_jis_g3444_all_have_required_fields():
    for s in get_jis_g3444_sections():
        assert s.standard == "JIS_G3444"
        assert s.nominal_size_inch
        assert s.dn > 0
        assert s.outside_diameter_mm > 0
        assert s.thickness_mm > 0
        assert s.weight_kg_per_m > 0


def test_jis_g3444_extends_to_400mm():
    sections = get_jis_g3444_sections()
    max_od = max(s.outside_diameter_mm for s in sections)
    assert max_od == 406.4  # DN 400 = 16"


def test_standards_overlap():
    """Both standards should share common sizes (e.g., 1/2" 21.7mm)."""
    tis = get_tis107_sections()
    jis = get_jis_g3444_sections()
    tis_keys = {(s.outside_diameter_mm, s.thickness_mm) for s in tis}
    jis_keys = {(s.outside_diameter_mm, s.thickness_mm) for s in jis}
    overlap = tis_keys & jis_keys
    assert len(overlap) > 10  # Should have significant overlap
