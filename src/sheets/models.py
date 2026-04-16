"""Pydantic models for Google Sheets data."""
from pydantic import BaseModel


class ExistingSheetRow(BaseModel):
    """A row from the 'CHS JIS M' sheet."""

    model: str  # e.g. '3/4" 27.2 x 2.0'
    date: str | None = None
    osh_code: str | None = None
    description: str | None = None
    weight_kg_6m: float | None = None
    weight_kg_per_m: float | None = None
    price_per_kg: float | None = None
    diameter_mm: float | None = None
    diameter_inch: str | None = None
    thickness_mm: float | None = None
    price_thb: float | None = None  # per piece (6m)
    cost_thb: float | None = None


class OutputSheetRow(BaseModel):
    """A row for the 'CHS JIS Claude' output sheet."""

    nominal_size_inch: str
    dn: int
    outside_diameter_mm: float
    thickness_mm: float
    standard: str  # "TIS_107", "JIS_G3444", or "BOTH"
    grade: str
    weight_kg_per_m: float
    cross_section_area_cm2: float | None = None
    moment_of_inertia_cm4: float | None = None
    section_modulus_cm3: float | None = None
    radius_of_gyration_cm: float | None = None
    osh_url: str | None = None
    price_thb: float | None = None  # per 6m piece
    price_per_meter: float | None = None
    price_per_kg: float | None = None
    last_updated: str | None = None

    def to_row(self) -> list:
        """Convert to a list for writing to Google Sheets."""
        return [
            self.nominal_size_inch,
            self.dn,
            self.outside_diameter_mm,
            self.thickness_mm,
            self.standard,
            self.grade,
            self.weight_kg_per_m,
            self.cross_section_area_cm2,
            self.moment_of_inertia_cm4,
            self.section_modulus_cm3,
            self.radius_of_gyration_cm,
            self.osh_url,
            self.price_thb,
            self.price_per_meter,
            self.price_per_kg,
            self.last_updated,
        ]

    @staticmethod
    def header_row() -> list[str]:
        return [
            "Nominal Size (inch)",
            "DN",
            "OD (mm)",
            "Thickness (mm)",
            "Standard",
            "Grade",
            "Weight (kg/m)",
            "Area (cm²)",
            "Ix (cm⁴)",
            "Wx (cm³)",
            "rx (cm)",
            "OSH URL",
            "Price (฿/piece)",
            "Price (฿/m)",
            "Price (฿/kg)",
            "Last Updated",
        ]
