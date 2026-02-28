"""
DSR|RIECT — Alert Generator
Scans KPI DataFrames, produces AlertRecord list sorted P1 first
"""

import logging
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from riect.alert_engine.priority_engine import upgrade_priority
from config import PRIORITY_LABELS

logger = logging.getLogger(__name__)


@dataclass
class AlertRecord:
    alert_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    session_id: str = ""
    priority: str = "P4"
    kpi_type: str = ""
    signal_type: str = ""
    dimension: str = ""
    dimension_value: str = ""
    kpi_value: float = 0.0
    threshold: float = 0.0
    gap: float = 0.0
    status: str = "OPEN"
    exception_text: str = ""
    recommended_action: str = ""
    action_owner: str = ""
    response_timeline: str = ""
    expected_impact: str = ""
    resolved: int = 0
    resolved_at: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


# KPI column mappings: (kpi_type, value_col, dimension_col, signal_type)
KPI_MAPPINGS = {
    "SPSF": {
        "value_col": "spsf",
        "dim_cols": ["store_name", "store_id"],
        "signal_type": "SPSF_BREACH",
        "threshold_key": "threshold",
    },
    "SELL_THRU": {
        "value_col": "sell_thru_pct",
        "dim_cols": ["category", "article_id", "store_name", "store_id"],
        "signal_type": "SELL_THRU_BREACH",
        "threshold_key": "threshold",
    },
    "DOI": {
        "value_col": "doi",
        "dim_cols": ["store_name", "store_id", "article_id"],
        "signal_type": "DOI_BREACH",
        "threshold_key": "threshold",
    },
    "MBQ": {
        "value_col": "compliance_pct",
        "dim_cols": ["article_id", "store_id"],
        "signal_type": "MBQ_BREACH",
        "threshold_key": "threshold",
    },
}


def generate_alerts(kpi_results: dict, session_id: str = "") -> list[AlertRecord]:
    """
    Generate AlertRecord list from KPI engine results.
    Returns alerts sorted P1 → P2 → P3 → P4.
    """
    alerts = []

    for kpi_name, engine_result in kpi_results.items():
        # Skip non-engine entries: combined_breaches (list), total_p1/p2/p3 (int), anomalies (dict without "available")
        if not isinstance(engine_result, dict) or "available" not in engine_result:
            continue
        kpi_upper = kpi_name.upper()
        if not engine_result.get("available"):
            continue

        breaches_df = engine_result.get("breaches")
        if breaches_df is None or breaches_df.empty:
            continue

        mapping = KPI_MAPPINGS.get(kpi_upper, {})
        value_col = mapping.get("value_col", "")
        dim_cols = mapping.get("dim_cols", [])
        signal_type = mapping.get("signal_type", f"{kpi_upper}_BREACH")

        if value_col not in breaches_df.columns:
            continue

        for _, row in breaches_df.iterrows():
            # Find dimension value
            dimension = "unknown"
            dim_val = "N/A"
            for dc in dim_cols:
                if dc in breaches_df.columns:
                    dimension = dc
                    dim_val = str(row.get(dc, "N/A"))
                    break

            kpi_value = float(row.get(value_col, 0) or 0)
            priority = str(row.get("priority", "P4"))
            gap = float(row.get("gap_to_target", 0) or row.get("gap_to_p1", 0) or 0)

            threshold = _get_threshold(kpi_upper, priority)
            exception_text = _build_exception_text(kpi_upper, dim_val, kpi_value, priority)

            alert = AlertRecord(
                session_id=session_id,
                priority=priority,
                kpi_type=kpi_upper,
                signal_type=signal_type,
                dimension=dimension,
                dimension_value=dim_val,
                kpi_value=round(kpi_value, 4),
                threshold=threshold,
                gap=round(abs(gap), 4),
                exception_text=exception_text,
            )
            alerts.append(alert.to_dict())

    # Apply compound upgrade rule
    alerts = upgrade_priority(alerts)

    # Sort by priority
    priority_order = {"P1": 0, "P2": 1, "P3": 2, "P4": 3}
    alerts.sort(key=lambda a: priority_order.get(a.get("priority", "P4"), 3))

    logger.info(
        f"Generated {len(alerts)} alerts: "
        f"P1={sum(1 for a in alerts if a['priority']=='P1')}, "
        f"P2={sum(1 for a in alerts if a['priority']=='P2')}, "
        f"P3={sum(1 for a in alerts if a['priority']=='P3')}"
    )
    return alerts


def _get_threshold(kpi_type: str, priority: str) -> float:
    from riect.alert_engine.priority_engine import get_thresholds
    thresholds = get_thresholds(kpi_type)
    return float(thresholds.get(priority, 0))


def _build_exception_text(kpi_type: str, dim_val: str, kpi_value: float, priority: str) -> str:
    label = PRIORITY_LABELS.get(priority, priority)
    templates = {
        "SPSF": f"{label}: {dim_val} SPSF at {kpi_value:.0f} — below {priority} threshold",
        "SELL_THRU": f"{label}: {dim_val} Sell-Through at {kpi_value*100:.1f}% — below {priority} threshold",
        "DOI": f"{label}: {dim_val} DOI at {kpi_value:.0f} days — above {priority} threshold",
        "MBQ": f"{label}: {dim_val} MBQ compliance at {kpi_value*100:.0f}% — {priority} shortfall",
    }
    return templates.get(kpi_type, f"{label}: {dim_val} {kpi_type}={kpi_value:.2f}")
