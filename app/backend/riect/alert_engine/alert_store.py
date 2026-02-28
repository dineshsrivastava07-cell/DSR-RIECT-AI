"""
DSR|RIECT — Alert Store
DB read/write for riect_alerts table
"""

import logging
from datetime import datetime, timezone
from typing import Any

from db import get_connection

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_alerts(alerts: list[dict]) -> int:
    """Insert new alerts into riect_alerts table. Returns count saved."""
    if not alerts:
        return 0

    conn = get_connection()
    try:
        saved = 0
        for alert in alerts:
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO riect_alerts
                       (alert_id, created_at, session_id, priority, kpi_type, signal_type,
                        dimension, dimension_value, kpi_value, threshold, gap, status,
                        exception_text, recommended_action, action_owner, response_timeline,
                        expected_impact, resolved, resolved_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        alert.get("alert_id", ""),
                        alert.get("created_at", _now()),
                        alert.get("session_id", ""),
                        alert.get("priority", "P4"),
                        alert.get("kpi_type", ""),
                        alert.get("signal_type", ""),
                        alert.get("dimension", ""),
                        alert.get("dimension_value", ""),
                        alert.get("kpi_value"),
                        alert.get("threshold"),
                        alert.get("gap"),
                        alert.get("status", "OPEN"),
                        alert.get("exception_text", ""),
                        alert.get("recommended_action", ""),
                        alert.get("action_owner", ""),
                        alert.get("response_timeline", ""),
                        alert.get("expected_impact", ""),
                        0,
                        "",
                    ),
                )
                saved += 1
            except Exception as e:
                logger.warning(f"Failed to save alert {alert.get('alert_id')}: {e}")
        conn.commit()
        return saved
    finally:
        conn.close()


def get_alerts(
    priority: str = None,
    resolved: bool = False,
    session_id: str = None,
    limit: int = 100,
) -> list[dict]:
    """Retrieve alerts with optional filters."""
    conn = get_connection()
    try:
        conditions = ["resolved = ?"]
        params: list = [1 if resolved else 0]

        if priority:
            conditions.append("priority = ?")
            params.append(priority)

        if session_id:
            conditions.append("session_id = ?")
            params.append(session_id)

        where = " AND ".join(conditions)
        order = "CASE priority WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END, created_at DESC"

        rows = conn.execute(
            f"SELECT * FROM riect_alerts WHERE {where} ORDER BY {order} LIMIT ?",
            params + [limit],
        ).fetchall()

        return [dict(row) for row in rows]
    finally:
        conn.close()


def resolve_alert(alert_id: str) -> bool:
    """Mark alert as resolved."""
    conn = get_connection()
    try:
        cur = conn.execute(
            "UPDATE riect_alerts SET resolved=1, resolved_at=? WHERE alert_id=?",
            (_now(), alert_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def get_alert_summary() -> dict:
    """Return alert counts by priority (open alerts only)."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT priority, COUNT(*) as count
               FROM riect_alerts
               WHERE resolved = 0
               GROUP BY priority
               ORDER BY CASE priority WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END"""
        ).fetchall()

        summary = {"P1": 0, "P2": 0, "P3": 0, "P4": 0, "total": 0}
        for row in rows:
            summary[row["priority"]] = row["count"]
            summary["total"] += row["count"]
        return summary
    finally:
        conn.close()


def clear_scan_alerts() -> int:
    """Delete all unresolved auto-scan alerts (session_id LIKE 'scan_%'). Returns deleted count."""
    conn = get_connection()
    try:
        cur = conn.execute(
            "DELETE FROM riect_alerts WHERE resolved = 0 AND session_id LIKE 'scan_%'"
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def get_alert_counts_by_kpi() -> dict:
    """Return P1/P2/P3 alert counts per KPI type (open alerts only)."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT kpi_type, priority, COUNT(*) as count
               FROM riect_alerts
               WHERE resolved = 0
               GROUP BY kpi_type, priority"""
        ).fetchall()

        result = {
            "SPSF":     {"P1": 0, "P2": 0, "P3": 0},
            "SELL_THRU": {"P1": 0, "P2": 0, "P3": 0},
            "DOI":      {"P1": 0, "P2": 0, "P3": 0},
            "MBQ":      {"P1": 0, "P2": 0, "P3": 0},
        }
        for row in rows:
            kpi = row["kpi_type"]
            pri = row["priority"]
            if kpi in result and pri in result[kpi]:
                result[kpi][pri] = row["count"]
        return result
    finally:
        conn.close()
