"""Pydantic models for Firestore collections."""
from datetime import datetime

from pydantic import BaseModel


class SectionDoc(BaseModel):
    """Document in the 'sections' collection — engineering data only."""

    type: str = "CHS"
    nominal_size_inch: str
    dn: int
    outside_diameter_mm: float
    thickness_mm: float
    standard: str
    grade: str
    weight_kg_per_m: float
    cross_section_area_cm2: float | None = None
    moment_of_inertia_cm4: float | None = None
    section_modulus_cm3: float | None = None
    radius_of_gyration_cm: float | None = None

    def doc_id(self) -> str:
        """Generate Firestore document ID."""
        size = self.nominal_size_inch.replace(" ", "").replace("/", "-")
        return f"CHS_{size}in_{self.thickness_mm}_{self.standard}"


class VendorDoc(BaseModel):
    """Document in the 'vendors' collection."""

    name: str
    url: str
    vendor_type: str  # "online_marketplace", "distributor", etc.
    scrape_enabled: bool = True
    last_synced: datetime | None = None


class VendorPriceDoc(BaseModel):
    """Document in the 'vendor_prices' collection."""

    vendor_id: str
    section_id: str
    product_name: str | None = None
    osh_code: str | None = None
    product_url: str | None = None
    price_thb: float | None = None
    price_per_meter: float | None = None
    price_per_kg: float | None = None
    unit: str | None = None
    product_type: str = "black_pipe"
    in_stock: bool = True
    last_scraped: datetime | None = None

    def doc_id(self) -> str:
        """Generate Firestore document ID."""
        return f"{self.vendor_id}_{self.section_id}"


class PriceHistoryDoc(BaseModel):
    """Document in the 'price_history' subcollection."""

    price_thb: float | None = None
    price_per_kg: float | None = None
    in_stock: bool = True
    scraped_at: datetime | None = None


class ScrapeRunDoc(BaseModel):
    """Document in the 'scrape_runs' collection."""

    vendor_id: str
    started_at: datetime
    completed_at: datetime | None = None
    products_scraped: int = 0
    sections_updated: int = 0
    prices_changed: int = 0
    errors: list[str] = []
