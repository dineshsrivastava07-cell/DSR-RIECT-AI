"""
DSR|RIECT — Store SqFt Master Store
Manages store floor area data in SQLite store_sqft table.
Used by SPSF engine to compute Sales Per Square Foot authentically.
"""

import csv
import logging
from datetime import datetime, timezone
from db import get_connection

logger = logging.getLogger(__name__)


def import_from_csv(csv_path: str) -> dict:
    """
    Import store sqft data from CSV into SQLite store_sqft table.
    Expected columns: Store_ID, SHRTNAME, Store_NAME, Square Feet Area, SITETYPE, City_Name
    Returns: {imported, skipped, total}
    """
    imported = 0
    skipped = 0
    now = datetime.now(timezone.utc).isoformat()

    try:
        with open(csv_path, encoding="utf-8-sig") as f:  # utf-8-sig handles BOM
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        logger.error(f"store_sqft CSV read failed: {e}")
        return {"error": str(e), "imported": 0}

    conn = get_connection()
    try:
        for row in rows:
            try:
                store_id = int(row.get("Store_ID", 0))
                sqft_raw = row.get("Square Feet Area", "0").strip()
                sqft = int(sqft_raw) if sqft_raw.isdigit() else 0
                shrtname = (row.get("SHRTNAME") or "").strip().upper()
                store_name = (row.get("Store_NAME") or "").strip()
                sitetype = (row.get("SITETYPE") or "").strip()
                city_name = (row.get("City_Name") or "").strip()

                if store_id <= 0:
                    skipped += 1
                    continue

                conn.execute(
                    """
                    INSERT INTO store_sqft
                        (store_id, store_name, shrtname, sitetype, floor_sqft, city_name, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(store_id) DO UPDATE SET
                        store_name=excluded.store_name,
                        shrtname=excluded.shrtname,
                        sitetype=excluded.sitetype,
                        floor_sqft=excluded.floor_sqft,
                        city_name=excluded.city_name,
                        updated_at=excluded.updated_at
                    """,
                    (store_id, store_name, shrtname, sitetype, sqft, city_name, now),
                )
                imported += 1
            except Exception as row_err:
                logger.warning(f"store_sqft row skip: {row_err} — row={row}")
                skipped += 1

        conn.commit()
        logger.info(f"store_sqft: imported={imported}, skipped={skipped}")
    except Exception as e:
        logger.error(f"store_sqft import failed: {e}")
        return {"error": str(e), "imported": imported}
    finally:
        conn.close()

    return {"imported": imported, "skipped": skipped, "total": len(rows)}


def get_sqft_lookup_by_store_id() -> dict:
    """Return {store_id (int): floor_sqft (int)} for all stores."""
    try:
        conn = get_connection()
        rows = conn.execute(
            "SELECT store_id, floor_sqft FROM store_sqft WHERE floor_sqft > 0"
        ).fetchall()
        conn.close()
        return {r["store_id"]: r["floor_sqft"] for r in rows}
    except Exception as e:
        logger.warning(f"get_sqft_lookup_by_store_id failed: {e}")
        return {}


def get_sqft_lookup_by_shrtname() -> dict:
    """Return {SHRTNAME (upper): floor_sqft (int)} for all stores."""
    try:
        conn = get_connection()
        rows = conn.execute(
            "SELECT shrtname, floor_sqft FROM store_sqft WHERE floor_sqft > 0 AND shrtname != ''"
        ).fetchall()
        conn.close()
        return {r["shrtname"]: r["floor_sqft"] for r in rows}
    except Exception as e:
        logger.warning(f"get_sqft_lookup_by_shrtname failed: {e}")
        return {}


def get_store_sqft_count() -> int:
    """Return number of stores with sqft data loaded."""
    try:
        conn = get_connection()
        count = conn.execute(
            "SELECT COUNT(*) FROM store_sqft WHERE floor_sqft > 0"
        ).fetchone()[0]
        conn.close()
        return count
    except Exception:
        return 0


def get_store_label_lookup() -> dict:
    """
    Return {store_id (int): {shrtname, store_name, city_name}} for label enrichment.
    Used by orchestrator to add human-readable store names when only STORE_ID is in result.
    """
    try:
        conn = get_connection()
        rows = conn.execute(
            "SELECT store_id, shrtname, store_name, city_name FROM store_sqft WHERE store_id > 0"
        ).fetchall()
        conn.close()
        return {
            r["store_id"]: {
                "shrtname": r["shrtname"] or "",
                "store_name": r["store_name"] or "",
                "city_name": r["city_name"] or "",
            }
            for r in rows
        }
    except Exception as e:
        logger.warning(f"get_store_label_lookup failed: {e}")
        return {}


def get_all_stores() -> list:
    """Return all store sqft rows as list of dicts."""
    try:
        conn = get_connection()
        rows = conn.execute(
            "SELECT * FROM store_sqft ORDER BY store_id"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.warning(f"get_all_stores failed: {e}")
        return []
