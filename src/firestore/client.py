"""Firestore client for the steel-sections database."""
import logging
from datetime import datetime, timezone

from google.cloud import firestore

from config.constants import (
    PRICE_HISTORY_SUBCOLLECTION,
    SCRAPE_RUNS_COLLECTION,
    SECTIONS_COLLECTION,
    VENDOR_PRICES_COLLECTION,
    VENDORS_COLLECTION,
)
from config.settings import settings
from src.firestore.models import (
    PriceHistoryDoc,
    ScrapeRunDoc,
    SectionDoc,
    VendorDoc,
    VendorPriceDoc,
)

logger = logging.getLogger(__name__)


def get_firestore_client() -> firestore.Client:
    """Get a Firestore client for the steel-sections database."""
    return firestore.Client(
        project=settings.gcp_project_id,
        database=settings.firestore_database,
    )


def upsert_sections(db: firestore.Client, sections: list[SectionDoc]) -> int:
    """Batch-upsert section documents. Returns count written."""
    coll = db.collection(SECTIONS_COLLECTION)
    batch = db.batch()
    count = 0

    for sec in sections:
        doc_ref = coll.document(sec.doc_id())
        batch.set(doc_ref, sec.model_dump(), merge=True)
        count += 1

        # Firestore batches max 500 operations
        if count % 400 == 0:
            batch.commit()
            batch = db.batch()

    if count % 400 != 0:
        batch.commit()

    logger.info(f"Upserted {count} sections to Firestore")
    return count


def ensure_vendor(db: firestore.Client, vendor: VendorDoc) -> None:
    """Create or update a vendor document."""
    coll = db.collection(VENDORS_COLLECTION)
    doc_ref = coll.document(vendor.name.lower().replace(" ", "_"))
    doc_ref.set(vendor.model_dump(), merge=True)
    logger.info(f"Ensured vendor: {vendor.name}")


def upsert_vendor_prices(
    db: firestore.Client,
    prices: list[VendorPriceDoc],
) -> tuple[int, int]:
    """Upsert vendor price documents and track price changes.

    Returns (updated_count, changed_count).
    """
    coll = db.collection(VENDOR_PRICES_COLLECTION)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    batch = db.batch()
    updated = 0
    changed = 0

    for vp in prices:
        doc_id = vp.doc_id()
        doc_ref = coll.document(doc_id)

        # Check if price changed
        existing = doc_ref.get()
        old_price = existing.to_dict().get("price_thb") if existing.exists else None
        if old_price is not None and old_price != vp.price_thb:
            changed += 1

        batch.set(doc_ref, vp.model_dump(), merge=True)

        # Write price history
        history_ref = doc_ref.collection(PRICE_HISTORY_SUBCOLLECTION).document(today)
        history = PriceHistoryDoc(
            price_thb=vp.price_thb,
            price_per_kg=vp.price_per_kg,
            in_stock=vp.in_stock,
            scraped_at=vp.last_scraped,
        )
        batch.set(history_ref, history.model_dump(), merge=True)

        updated += 1
        if updated % 200 == 0:  # 2 ops per price (doc + history)
            batch.commit()
            batch = db.batch()

    if updated % 200 != 0:
        batch.commit()

    logger.info(f"Upserted {updated} vendor prices ({changed} price changes)")
    return updated, changed


def record_scrape_run(db: firestore.Client, run: ScrapeRunDoc) -> str:
    """Record a scrape run. Returns the document ID."""
    coll = db.collection(SCRAPE_RUNS_COLLECTION)
    _, doc_ref = coll.add(run.model_dump())
    logger.info(f"Recorded scrape run: {doc_ref.id}")
    return doc_ref.id


def get_all_sections(db: firestore.Client) -> list[dict]:
    """Read all sections, sorted by OD."""
    coll = db.collection(SECTIONS_COLLECTION)
    docs = coll.order_by("outside_diameter_mm").stream()
    return [doc.to_dict() for doc in docs]


def get_sections_by_size(db: firestore.Client, size_inch: str) -> list[dict]:
    """Get sections for a given nominal inch size."""
    coll = db.collection(SECTIONS_COLLECTION)
    docs = (
        coll.where("nominal_size_inch", "==", size_inch)
        .order_by("thickness_mm")
        .stream()
    )
    return [doc.to_dict() for doc in docs]
