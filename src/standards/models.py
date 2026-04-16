"""Data model for steel section properties from standards."""
from pydantic import BaseModel


class SteelSection(BaseModel):
    """A single CHS entry from TIS 107 or JIS G3444."""

    standard: str  # "TIS_107" or "JIS_G3444"
    grade: str  # "HS41", "HS50", "HS51", "STK400", "STK490"
    nominal_size_inch: str  # "1/2", "3/4", "1", "1 1/4", etc.
    dn: int  # Diameter Nominal (metric number)
    outside_diameter_mm: float
    thickness_mm: float
    weight_kg_per_m: float
    cross_section_area_cm2: float | None = None
    moment_of_inertia_cm4: float | None = None
    section_modulus_cm3: float | None = None
    radius_of_gyration_cm: float | None = None
