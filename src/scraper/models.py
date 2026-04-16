"""Pydantic models for scraped product data."""
from datetime import datetime

from pydantic import BaseModel


class ScrapedProduct(BaseModel):
    """A product scraped from OneStockHome."""

    product_name: str
    osh_code: str | None = None
    url: str | None = None
    size_inch: str | None = None
    outside_diameter_mm: float | None = None
    thickness_mm: float | None = None
    price_thb: float | None = None
    unit: str | None = None  # "เส้น", "ตัว", etc.
    product_type: str = "unknown"  # "black_pipe", "galvanized", "gasket", "fitting"
    in_stock: bool = True
    scraped_at: datetime | None = None
