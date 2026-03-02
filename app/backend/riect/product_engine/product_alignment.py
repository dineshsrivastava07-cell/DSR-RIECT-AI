"""
DSR|RIECT — Product Alignment Engine

Builds the complete product identity from 3 ClickHouse tables:
  pos_transactional_data  → Division / Section / Department / Article / Style / Size / Color
  vitem_data              → exact Cost (RATE) + exact MRP + Item Description + Supplier
  inventory_current       → Option Code

Critical rules:
  - RATE and MRP are String columns in vitem_data — always cast with toFloat64OrNull()
  - OPTION_CODE lives in inventory_current ONLY (not pos)
  - Use anyLast() for dedup in GROUP BY aggregations
  - TRADING_F applied to all product KPI queries
  - coalesce(ITEM_NAME, ARTICLENAME) — ITEM_NAME may be blank
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

_IST = timezone(timedelta(hours=5, minutes=30))

# ─── Core alignment SQL ────────────────────────────────────────────────────────
# Joins pos + vitem_data + inventory_current on ICODE
# Scoped to last 90 days so we only see active/traded ICODEs

_ALIGNMENT_SQL = """
SELECT
    p.ICODE,
    anyLast(p.ARTICLECODE)                                          AS article_code,
    anyLast(p.ARTICLENAME)                                          AS article_name,
    anyLast(p.DIVISION)                                             AS division,
    anyLast(p.SECTION)                                              AS section,
    anyLast(p.DEPARTMENT)                                           AS department,
    anyLast(inv_opt.OPTION_CODE)                                    AS option_code,
    anyLast(toFloat64OrNull(v.RATE))                                AS cost_price,
    anyLast(toFloat64OrNull(v.MRP))                                 AS mrp,
    coalesce(anyLast(v.ITEM_NAME), anyLast(p.ARTICLENAME))          AS item_description,
    anyLast(v.PARTYNAME)                                            AS supplier_name,
    anyLast(p.STYLE_OR_PATTERN)                                     AS style_or_pattern,
    anyLast(p.SIZE)                                                 AS size,
    anyLast(p.COLOR)                                                AS color
FROM `vmart_sales`.`pos_transactional_data` p
LEFT JOIN (
    SELECT ICODE,
           anyLast(toFloat64OrNull(RATE))   AS RATE,
           anyLast(toFloat64OrNull(MRP))    AS MRP,
           anyLast(ITEM_NAME)               AS ITEM_NAME,
           anyLast(PARTYNAME)               AS PARTYNAME
    FROM `vmart_product`.`vitem_data`
    GROUP BY ICODE
) v ON p.ICODE = v.ICODE
LEFT JOIN (
    SELECT ICODE, anyLast(OPTION_CODE) AS OPTION_CODE
    FROM `vmart_product`.`inventory_current`
    GROUP BY ICODE
) inv_opt ON p.ICODE = inv_opt.ICODE
WHERE toDate(p.BILLDATE) >= toDate('{date}') - INTERVAL 90 DAY
  AND p.ICODE IS NOT NULL AND p.ICODE != ''
  AND p.DIVISION NOT IN ('NON TRADING','NON-TRADING','NON TRADE','OTHERS','OTHER',
      'STAFF WELFARE','STAFF UNIFORM','ASSETS','ASSETS & CONSUMABLES','CONSUMABLES')
GROUP BY p.ICODE
LIMIT {limit}
"""


def _get_latest_date() -> str:
    """Return the most recent complete sales date from ClickHouse."""
    try:
        from clickhouse.connector import get_client
        ch = get_client()
        r = ch.query(
            "SELECT toDate(BILLDATE) AS dt "
            "FROM `vmart_sales`.`pos_transactional_data` "
            "GROUP BY dt HAVING COUNT(DISTINCT BILLNO) >= 10000 "
            "ORDER BY dt DESC LIMIT 1"
        )
        if r.result_rows:
            return str(r.result_rows[0][0])
    except Exception as e:
        logger.warning(f"Latest date lookup failed: {e}")
    return datetime.now(_IST).strftime("%Y-%m-%d")


# ─── Public API ────────────────────────────────────────────────────────────────

def build_product_alignment(limit: int = 5000) -> list[dict]:
    """
    Fetch full product alignment from ClickHouse.
    Returns list of aligned product rows with ICODE, hierarchy, cost, MRP, option_code, etc.
    """
    date = _get_latest_date()
    sql = _ALIGNMENT_SQL.format(date=date, limit=limit)
    try:
        from clickhouse.query_runner import run_query
        result = run_query(sql)
        if "error" in result:
            logger.error(f"build_product_alignment query error: {result['error']}")
            return []
        return result.get("data", [])
    except Exception as e:
        logger.error(f"build_product_alignment failed: {e}")
        return []


def get_product_hierarchy() -> dict:
    """
    Return Division → Section → Department tree as nested dict with ICODE counts.
    Reads from SQLite cache first; falls back to a ClickHouse summary query.

    Returns:
        {
          "MENS": {
            "BOTTOM WEAR": {
              "JEANS": {"count": 203},
              ...
            }
          },
          ...
        }
    """
    rows = get_cached_products(limit=10000)
    if not rows:
        rows = build_product_alignment(limit=5000)

    tree: dict = {}
    for r in rows:
        div  = (r.get("division") or "").strip()
        sec  = (r.get("section") or "").strip()
        dept = (r.get("department") or "").strip()
        if not div:
            continue
        tree.setdefault(div, {}).setdefault(sec, {}).setdefault(dept, {"count": 0})
        tree[div][sec][dept]["count"] += 1
    return tree


def search_products(
    query: str = "",
    division: str = "",
    section: str = "",
    department: str = "",
    limit: int = 100,
) -> list[dict]:
    """
    Search products by any text field OR hierarchy filters.
    Reads from SQLite cache (fast — no ClickHouse call).
    Returns aligned rows matching all supplied filters.
    """
    rows = get_cached_products(division=division, section=section,
                               department=department, limit=5000)
    if not query:
        return rows[:limit]

    q = query.lower()
    matched = []
    for r in rows:
        haystack = " ".join([
            str(r.get("icode", "")),
            str(r.get("article_code", "")),
            str(r.get("article_name", "")),
            str(r.get("item_description", "")),
            str(r.get("supplier_name", "")),
            str(r.get("option_code", "")),
        ]).lower()
        if q in haystack:
            matched.append(r)
        if len(matched) >= limit:
            break
    return matched


def get_product_details(icode: str) -> Optional[dict]:
    """
    Get full alignment details for a single ICODE.
    Checks SQLite cache first; if not found, runs a targeted ClickHouse query.
    """
    # Try cache first
    from db import get_connection
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM product_alignment WHERE icode = ?", (icode,))
        row = cur.fetchone()
        conn.close()
        if row:
            return dict(row)
    except Exception as e:
        logger.warning(f"Cache lookup for {icode} failed: {e}")

    # Fallback: live ClickHouse query
    date = _get_latest_date()
    sql = _ALIGNMENT_SQL.format(date=date, limit=1).replace(
        "LIMIT 1",
        f"HAVING p.ICODE = '{icode}' LIMIT 1"
    )
    # Simpler targeted query
    sql = f"""
    SELECT
        p.ICODE,
        anyLast(p.ARTICLECODE)                                      AS article_code,
        anyLast(p.ARTICLENAME)                                      AS article_name,
        anyLast(p.DIVISION)                                         AS division,
        anyLast(p.SECTION)                                          AS section,
        anyLast(p.DEPARTMENT)                                       AS department,
        anyLast(inv_opt.OPTION_CODE)                                AS option_code,
        anyLast(toFloat64OrNull(v.RATE))                            AS cost_price,
        anyLast(toFloat64OrNull(v.MRP))                             AS mrp,
        coalesce(anyLast(v.ITEM_NAME), anyLast(p.ARTICLENAME))      AS item_description,
        anyLast(v.PARTYNAME)                                        AS supplier_name,
        anyLast(p.STYLE_OR_PATTERN)                                 AS style_or_pattern,
        anyLast(p.SIZE)                                             AS size,
        anyLast(p.COLOR)                                            AS color
    FROM `vmart_sales`.`pos_transactional_data` p
    LEFT JOIN (
        SELECT ICODE, anyLast(toFloat64OrNull(RATE)) AS RATE,
               anyLast(toFloat64OrNull(MRP)) AS MRP,
               anyLast(ITEM_NAME) AS ITEM_NAME, anyLast(PARTYNAME) AS PARTYNAME
        FROM `vmart_product`.`vitem_data` GROUP BY ICODE
    ) v ON p.ICODE = v.ICODE
    LEFT JOIN (
        SELECT ICODE, anyLast(OPTION_CODE) AS OPTION_CODE
        FROM `vmart_product`.`inventory_current` GROUP BY ICODE
    ) inv_opt ON p.ICODE = inv_opt.ICODE
    WHERE p.ICODE = '{icode}'
    GROUP BY p.ICODE
    LIMIT 1
    """
    try:
        from clickhouse.query_runner import run_query
        result = run_query(sql)
        data = result.get("data", [])
        return data[0] if data else None
    except Exception as e:
        logger.error(f"get_product_details({icode}) failed: {e}")
        return None


def cache_product_alignment(rows: list[dict]) -> int:
    """
    Upsert product alignment rows into SQLite product_alignment table.
    Returns count of rows saved.
    """
    if not rows:
        return 0

    from db import get_connection
    now = datetime.now(_IST).isoformat()
    saved = 0
    try:
        conn = get_connection()
        cur = conn.cursor()
        for r in rows:
            cur.execute(
                """
                INSERT OR REPLACE INTO product_alignment
                    (icode, article_code, article_name, division, section, department,
                     option_code, cost_price, mrp, item_description, supplier_name,
                     style_or_pattern, size, color, cached_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(r.get("ICODE", r.get("icode", ""))).strip(),
                    r.get("article_code", ""),
                    r.get("article_name", ""),
                    r.get("division", ""),
                    r.get("section", ""),
                    r.get("department", ""),
                    r.get("option_code", ""),
                    r.get("cost_price"),
                    r.get("mrp"),
                    r.get("item_description", ""),
                    r.get("supplier_name", ""),
                    r.get("style_or_pattern", ""),
                    r.get("size", ""),
                    r.get("color", ""),
                    now,
                ),
            )
            saved += 1
        conn.commit()
        conn.close()
        logger.info(f"cache_product_alignment: {saved} rows upserted")
    except Exception as e:
        logger.error(f"cache_product_alignment failed: {e}")
    return saved


def get_cached_products(
    division: str = "",
    section: str = "",
    department: str = "",
    limit: int = 200,
) -> list[dict]:
    """
    Read product alignment rows from SQLite cache (fast — no ClickHouse call).
    Optionally filter by division / section / department (case-insensitive).
    """
    from db import get_connection
    try:
        conn = get_connection()
        cur = conn.cursor()
        clauses = []
        params: list = []
        if division:
            clauses.append("UPPER(division) = UPPER(?)")
            params.append(division)
        if section:
            clauses.append("UPPER(section) = UPPER(?)")
            params.append(section)
        if department:
            clauses.append("UPPER(department) = UPPER(?)")
            params.append(department)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        cur.execute(
            f"SELECT * FROM product_alignment {where} ORDER BY article_name ASC LIMIT ?",
            params,
        )
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception as e:
        logger.warning(f"get_cached_products failed: {e}")
        return []


def refresh_alignment_cache() -> dict:
    """
    Fetch full product alignment from ClickHouse and write to SQLite cache.
    Returns {saved, duration_s}.
    """
    t0 = time.time()
    rows = build_product_alignment(limit=5000)
    if not rows:
        return {"saved": 0, "duration_s": round(time.time() - t0, 2), "error": "No rows returned"}
    saved = cache_product_alignment(rows)
    duration = round(time.time() - t0, 2)
    logger.info(f"refresh_alignment_cache: {saved} rows saved in {duration}s")
    return {"saved": saved, "duration_s": duration}
