"""Steel Fabrication — FastAPI service for steel section data."""
import logging
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from config.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Steel Fabrication API",
    description="Steel section database with TIS 107 / JIS G3444 standards and live pricing",
    version=settings.version,
)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/")
async def root():
    return {
        "service": settings.service_name,
        "version": settings.version,
    }


@app.post("/sync")
async def sync():
    """Trigger a full sync: scrape → merge → write sheet → upsert Firestore."""
    from src.pipeline.sync import run_full_sync

    try:
        summary = run_full_sync()
        return JSONResponse(content=summary)
    except Exception as e:
        logger.exception("Sync failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sections")
async def list_sections():
    """List all steel sections from Firestore."""
    from src.firestore.client import get_all_sections, get_firestore_client

    try:
        db = get_firestore_client()
        sections = get_all_sections(db)
        return {"count": len(sections), "sections": sections}
    except Exception as e:
        logger.exception("Failed to read sections")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sections/{size_inch}")
async def get_sections_by_size(size_inch: str):
    """Get sections for a given nominal inch size (e.g., '1', '1 1/2', '2')."""
    from src.firestore.client import get_sections_by_size as _get_by_size
    from src.firestore.client import get_firestore_client

    try:
        db = get_firestore_client()
        size = size_inch.replace("%20", " ")
        sections = _get_by_size(db, size)
        return {"size_inch": size, "count": len(sections), "sections": sections}
    except Exception as e:
        logger.exception(f"Failed to read sections for size {size_inch}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/standards")
async def list_standards():
    """List all standard section data (no Firestore needed)."""
    from src.standards.jis_g3444 import get_jis_g3444_sections
    from src.standards.tis107 import get_tis107_sections

    tis = get_tis107_sections()
    jis = get_jis_g3444_sections()
    return {
        "tis107_count": len(tis),
        "jis_g3444_count": len(jis),
        "total": len(tis) + len(jis),
        "tis107": [s.model_dump() for s in tis],
        "jis_g3444": [s.model_dump() for s in jis],
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=settings.port)
