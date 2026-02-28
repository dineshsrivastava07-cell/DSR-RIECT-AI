"""
DSR|RIECT — RIECT Plan Target Store
Manages KPI targets in SQLite riect_plan table.
Falls back to config.py defaults when no plan entry exists.
"""

import logging
from datetime import datetime, timezone
from db import get_connection
from config import SPSF_THRESHOLDS, SELL_THRU_THRESHOLDS, DOI_THRESHOLDS, MBQ_THRESHOLDS, UPT_THRESHOLDS

logger = logging.getLogger(__name__)

# Map KPI type to config.py fallback
_CONFIG_DEFAULTS = {
    "SPSF": SPSF_THRESHOLDS,
    "SELL_THRU": SELL_THRU_THRESHOLDS,
    "DOI": DOI_THRESHOLDS,
    "MBQ": MBQ_THRESHOLDS,
    "UPT": UPT_THRESHOLDS,
}


def get_kpi_targets(kpi_type: str, dimension: str = "global", dimension_value: str = "") -> dict:
    """
    Return thresholds for a KPI.
    Looks up: specific dimension_value → global override → config.py defaults.
    Returns dict with P1, P2, P3, target keys.
    """
    kpi_type = kpi_type.upper()
    rows = []

    try:
        conn = get_connection()
        cur = conn.execute(
            """
            SELECT p1_threshold, p2_threshold, p3_threshold, target
            FROM riect_plan
            WHERE kpi_type = ?
              AND dimension = ?
              AND dimension_value = ?
            LIMIT 1
            """,
            (kpi_type, dimension, dimension_value),
        )
        rows = cur.fetchall()
        conn.close()
    except Exception as e:
        logger.warning(f"riect_plan lookup failed: {e}")

    if rows:
        row = rows[0]
        return {
            "P1": row["p1_threshold"],
            "P2": row["p2_threshold"],
            "P3": row["p3_threshold"],
            "target": row["target"],
            "source": "riect_plan",
        }

    # Try global fallback from riect_plan
    if dimension != "global" or dimension_value:
        return get_kpi_targets(kpi_type, "global", "")

    # Final fallback: config.py defaults
    defaults = _CONFIG_DEFAULTS.get(kpi_type, {})
    return {**defaults, "source": "config_default"} if defaults else {}


def set_kpi_targets(
    kpi_type: str,
    p1: float,
    p2: float,
    p3: float,
    target: float,
    dimension: str = "global",
    dimension_value: str = "",
    period: str = "",
    notes: str = "",
) -> bool:
    """
    Upsert a KPI target row into riect_plan.
    Returns True on success.
    """
    kpi_type = kpi_type.upper()
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn = get_connection()
        conn.execute(
            """
            INSERT INTO riect_plan
                (kpi_type, dimension, dimension_value, p1_threshold, p2_threshold,
                 p3_threshold, target, period, notes, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(kpi_type, dimension, dimension_value)
            DO UPDATE SET
                p1_threshold=excluded.p1_threshold,
                p2_threshold=excluded.p2_threshold,
                p3_threshold=excluded.p3_threshold,
                target=excluded.target,
                period=excluded.period,
                notes=excluded.notes,
                updated_at=excluded.updated_at
            """,
            (kpi_type, dimension, dimension_value, p1, p2, p3, target, period, notes, now),
        )
        conn.commit()
        conn.close()
        logger.info(f"RIECT-Plan: saved {kpi_type}/{dimension}/{dimension_value or 'global'}")
        return True
    except Exception as e:
        logger.error(f"riect_plan save failed: {e}")
        return False


def get_all_plan_targets() -> list:
    """Return all rows from riect_plan as list of dicts."""
    try:
        conn = get_connection()
        cur = conn.execute(
            "SELECT * FROM riect_plan ORDER BY kpi_type, dimension, dimension_value"
        )
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception as e:
        logger.warning(f"get_all_plan_targets failed: {e}")
        return []


def delete_kpi_target(kpi_type: str, dimension: str = "global", dimension_value: str = "") -> bool:
    """Delete a specific target row."""
    try:
        conn = get_connection()
        conn.execute(
            "DELETE FROM riect_plan WHERE kpi_type=? AND dimension=? AND dimension_value=?",
            (kpi_type.upper(), dimension, dimension_value),
        )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"delete_kpi_target failed: {e}")
        return False


def get_plan_summary() -> dict:
    """Return current plan vs config defaults for UI display."""
    result = {}
    for kpi in ["SPSF", "SELL_THRU", "DOI", "MBQ", "UPT"]:
        targets = get_kpi_targets(kpi)
        defaults = _CONFIG_DEFAULTS.get(kpi, {})
        result[kpi] = {
            "current": targets,
            "config_default": defaults,
            "overridden": targets.get("source") == "riect_plan",
        }
    return result
