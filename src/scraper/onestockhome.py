"""Scraper for OneStockHome round pipe products."""
import logging
import re
import time
from datetime import datetime, timezone

import httpx
from bs4 import BeautifulSoup

from config.constants import (
    INCH_TO_OD,
    ONESTOCKHOME_BASE_URL,
    ONESTOCKHOME_TABLE_URL,
    SCRAPE_DELAY_SECONDS,
)
from src.scraper.models import ScrapedProduct

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "th,en;q=0.9",
}


def _classify_product(name: str) -> str:
    """Classify a product type from its Thai name."""
    lower = name.lower()
    if "gasket" in lower or "แกสเก็ท" in lower:
        return "gasket"
    if "ข้อต่อ" in lower or "fitting" in lower:
        return "fitting"
    if "ชุบ" in lower or "galvaniz" in lower or "กัลวาไนซ์" in lower:
        return "galvanized"
    if "p-coat" in lower or "พีโค้ท" in lower:
        return "p_coat"
    if "สเตนเลส" in lower or "stainless" in lower:
        return "stainless"
    if "เหล็กกลม" in lower or "ท่อเหล็ก" in lower:
        return "black_pipe"
    return "unknown"


def _parse_size_from_name(name: str) -> tuple[str | None, float | None]:
    """Extract inch size and OD from a Thai product name.

    Returns (size_inch, od_mm).
    """
    # Match patterns like "1 1/2 นิ้ว", "3/4 นิ้ว", "2 นิ้ว"
    match = re.search(r"(\d+\s*\d*/?\d*)\s*นิ้ว", name)
    if not match:
        # Try English inch patterns
        match = re.search(r'(\d+\s*\d*/?\d*)\s*["\']', name)
    if not match:
        return None, None

    size_str = match.group(1).strip()
    # Normalize: "1 1/2" stays, "0.5" → "1/2" etc.
    od = INCH_TO_OD.get(size_str)
    return size_str, od


def _parse_thickness_from_name(name: str) -> float | None:
    """Extract thickness in mm from a Thai product name."""
    # Match "หนา X มม." or "X มม." in context
    match = re.search(r"หนา\s*(\d+\.?\d*)\s*มม", name)
    if match:
        return float(match.group(1))
    # Match "OD. XX.XX มม. Y.Y มม." pattern (second mm is thickness)
    match = re.search(r"OD\.\s*[\d.]+\s*มม\.?\s*(\d+\.?\d*)\s*มม", name)
    if match:
        return float(match.group(1))
    return None


def _parse_price(price_text: str) -> float | None:
    """Parse price from text like '286.08' or '1,523.30'."""
    cleaned = price_text.strip().replace(",", "").replace("฿", "").replace("บาท", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def scrape_page(client: httpx.Client, page: int) -> list[ScrapedProduct]:
    """Scrape a single page of the product table."""
    url = f"{ONESTOCKHOME_TABLE_URL}?page={page}"
    resp = client.get(url, headers=_HEADERS, follow_redirects=True)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    now = datetime.now(timezone.utc)
    products = []

    # Find product rows — each product is in a list item or table row
    # OneStockHome uses div-based layout for product table
    product_elements = soup.select(
        "[class*='product-item'], [class*='item-row'], "
        "div[class*='flex'][class*='items-center']"
    )

    if not product_elements:
        # Fallback: try to find links with product URLs
        for link in soup.find_all("a", href=re.compile(r"/th/products/")):
            name = link.get_text(strip=True)
            if not name or len(name) < 5:
                continue

            href = link.get("href", "")
            if not href.startswith("http"):
                href = ONESTOCKHOME_BASE_URL + href

            # Try to find price near this element
            parent = link.find_parent(["div", "tr", "li"])
            price = None
            if parent:
                price_el = parent.find(
                    string=re.compile(r"[\d,]+\.\d{2}")
                )
                if price_el:
                    price = _parse_price(price_el.strip())

            # Find OSH code
            code_match = re.search(r"รหัสสินค้า\s*(\d+)", parent.get_text() if parent else "")
            osh_code = code_match.group(1) if code_match else None

            size_inch, od_mm = _parse_size_from_name(name)
            thickness = _parse_thickness_from_name(name)

            products.append(
                ScrapedProduct(
                    product_name=name,
                    osh_code=osh_code,
                    url=href,
                    size_inch=size_inch,
                    outside_diameter_mm=od_mm,
                    thickness_mm=thickness,
                    price_thb=price,
                    unit="เส้น" if "เส้น" in (parent.get_text() if parent else "") else None,
                    product_type=_classify_product(name),
                    scraped_at=now,
                )
            )

    logger.info(f"Page {page}: scraped {len(products)} products")
    return products


def scrape_all_pages(max_pages: int = 10) -> list[ScrapedProduct]:
    """Scrape all pages of the OneStockHome round pipe product table."""
    all_products: list[ScrapedProduct] = []

    with httpx.Client(timeout=30.0) as client:
        page = 1
        while page <= max_pages:
            try:
                products = scrape_page(client, page)
                if not products:
                    logger.info(f"No products on page {page}, stopping")
                    break
                all_products.extend(products)
                page += 1
                time.sleep(SCRAPE_DELAY_SECONDS)
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error on page {page}: {e}")
                break
            except Exception as e:
                logger.error(f"Error scraping page {page}: {e}")
                break

    logger.info(f"Total scraped: {len(all_products)} products from {page - 1} pages")
    return all_products


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    products = scrape_all_pages()
    for p in products:
        print(f"{p.product_name[:60]:60s} | {p.size_inch or '?':>6s} | "
              f"t={p.thickness_mm or '?'} | ฿{p.price_thb or '?'} | {p.product_type}")
