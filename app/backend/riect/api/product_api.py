"""
DSR|RIECT — Product Alignment REST API

Endpoints:
  GET  /api/product/alignment   — paginated full alignment with optional filters
  GET  /api/product/hierarchy   — Division → Section → Department tree
  GET  /api/product/search      — text + hierarchy search
  GET  /api/product/{icode}     — single ICODE full details
  POST /api/product/refresh     — trigger ClickHouse → SQLite cache refresh
"""

from __future__ import annotations

import logging
import math
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/product", tags=["product"])


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _margin(mrp, cost) -> Optional[float]:
    """Compute gross margin %: (MRP - Cost) / MRP * 100. Returns None if data missing."""
    try:
        m = float(mrp)
        c = float(cost)
        if m > 0:
            return round((m - c) / m * 100, 1)
    except (TypeError, ValueError, ZeroDivisionError):
        pass
    return None


def _enrich(row: dict) -> dict:
    """Add computed margin% to a product row."""
    row = dict(row)
    row["margin_pct"] = _margin(row.get("mrp"), row.get("cost_price"))
    return row


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.get("/alignment", summary="Paginated product alignment")
async def get_alignment(
    division:   str = Query("", description="Filter by division (case-insensitive)"),
    section:    str = Query("", description="Filter by section"),
    department: str = Query("", description="Filter by department"),
    page:       int = Query(1,  ge=1, description="Page number"),
    page_size:  int = Query(100, ge=1, le=500, description="Rows per page"),
):
    """
    Return paginated, optionally filtered product alignment from SQLite cache.

    Response:
        {total, page, page_size, filters, data: [...]}
    """
    from riect.product_engine.product_alignment import get_cached_products

    # Fetch all matching rows (up to 10000 for pagination)
    all_rows = get_cached_products(
        division=division, section=section, department=department, limit=10000
    )
    total = len(all_rows)
    total_pages = math.ceil(total / page_size) if total else 1
    start = (page - 1) * page_size
    page_rows = [_enrich(r) for r in all_rows[start: start + page_size]]

    return {
        "total":      total,
        "page":       page,
        "page_size":  page_size,
        "total_pages": total_pages,
        "filters":    {"division": division, "section": section, "department": department},
        "data":       page_rows,
    }


@router.get("/hierarchy", summary="Division → Section → Department tree")
async def get_hierarchy():
    """
    Return the full product hierarchy as a nested dict with ICODE counts.

    Response: {"MENS": {"BOTTOM WEAR": {"JEANS": {"count": 203}, ...}}, ...}
    """
    from riect.product_engine.product_alignment import get_product_hierarchy
    tree = get_product_hierarchy()
    return {"hierarchy": tree, "division_count": len(tree)}


@router.get("/search", summary="Search products by text or hierarchy")
async def search_products(
    q:          str = Query("", description="Free-text search (ICODE, article, description, supplier)"),
    division:   str = Query("", description="Filter by division"),
    section:    str = Query("", description="Filter by section"),
    department: str = Query("", description="Filter by department"),
    limit:      int = Query(100, ge=1, le=500, description="Max results"),
):
    """
    Search product alignment by free text and/or hierarchy filters.
    Reads from SQLite cache — no ClickHouse call.
    """
    from riect.product_engine.product_alignment import search_products as _search
    rows = _search(query=q, division=division, section=section, department=department, limit=limit)
    return {
        "query":   q,
        "filters": {"division": division, "section": section, "department": department},
        "count":   len(rows),
        "data":    [_enrich(r) for r in rows],
    }


@router.get("/{icode}", summary="Get full details for a single ICODE")
async def get_product(icode: str):
    """
    Return full product alignment for a single ICODE.
    Checks SQLite cache first; falls back to live ClickHouse query.
    """
    from riect.product_engine.product_alignment import get_product_details
    row = get_product_details(icode.strip())
    if not row:
        raise HTTPException(status_code=404, detail=f"ICODE '{icode}' not found")
    return {"icode": icode, "data": _enrich(row)}


@router.post("/refresh", summary="Refresh product alignment cache from ClickHouse")
async def refresh_cache():
    """
    Trigger a full refresh of the product_alignment SQLite cache from ClickHouse.
    Fetches up to 5,000 ICODEs (last 90 days of trading activity).

    Returns: {saved, duration_s}
    """
    from riect.product_engine.product_alignment import refresh_alignment_cache
    try:
        result = refresh_alignment_cache()
        if "error" in result:
            raise HTTPException(status_code=502, detail=result["error"])
        return {"status": "ok", **result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Cache refresh failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
