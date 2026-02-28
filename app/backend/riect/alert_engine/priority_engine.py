"""
DSR|RIECT — Priority Engine
P1-P4 classification rules for KPI breaches
"""

from config import SPSF_THRESHOLDS, SELL_THRU_THRESHOLDS, DOI_THRESHOLDS, MBQ_THRESHOLDS

PRIORITY_RULES = {
    "SPSF": {
        "P1": lambda v: v < SPSF_THRESHOLDS["P1"],
        "P2": lambda v: SPSF_THRESHOLDS["P1"] <= v < SPSF_THRESHOLDS["P2"],
        "P3": lambda v: SPSF_THRESHOLDS["P2"] <= v < SPSF_THRESHOLDS["P3"],
        "P4": lambda v: v >= SPSF_THRESHOLDS["P3"],
    },
    "SELL_THRU": {
        "P1": lambda v: v < SELL_THRU_THRESHOLDS["P1"],
        "P2": lambda v: SELL_THRU_THRESHOLDS["P1"] <= v < SELL_THRU_THRESHOLDS["P2"],
        "P3": lambda v: SELL_THRU_THRESHOLDS["P2"] <= v < SELL_THRU_THRESHOLDS["P3"],
        "P4": lambda v: v >= SELL_THRU_THRESHOLDS["P3"],
    },
    "DOI": {
        "P1": lambda v: v > DOI_THRESHOLDS["P1"],
        "P2": lambda v: DOI_THRESHOLDS["P2"] < v <= DOI_THRESHOLDS["P1"],
        "P3": lambda v: DOI_THRESHOLDS["P3"] < v <= DOI_THRESHOLDS["P2"],
        "P4": lambda v: v <= DOI_THRESHOLDS["P3"],
    },
    "MBQ": {
        "P1": lambda v: v < MBQ_THRESHOLDS["critical_shortfall_pct"],
        "P2": lambda v: MBQ_THRESHOLDS["critical_shortfall_pct"] <= v < MBQ_THRESHOLDS["high_shortfall_pct"],
        "P3": lambda v: MBQ_THRESHOLDS["high_shortfall_pct"] <= v < MBQ_THRESHOLDS["medium_shortfall_pct"],
        "P4": lambda v: v >= MBQ_THRESHOLDS["medium_shortfall_pct"],
    },
}


def classify_priority(kpi_type: str, kpi_value: float) -> str:
    """Classify a single KPI value into P1-P4."""
    rules = PRIORITY_RULES.get(kpi_type.upper(), {})
    for priority in ["P1", "P2", "P3", "P4"]:
        rule = rules.get(priority)
        if rule and rule(kpi_value):
            return priority
    return "P4"


def upgrade_priority(alerts: list) -> list:
    """
    Compound rule: if a dimension has dual P2 breaches → upgrade to P1.
    Modifies alert list in-place, returns updated list.
    """
    from collections import defaultdict
    # Count P2 alerts per dimension_value
    p2_counts: dict[str, int] = defaultdict(int)
    for alert in alerts:
        if alert.get("priority") == "P2":
            p2_counts[alert.get("dimension_value", "")] += 1

    # Upgrade to P1 if dimension has 2+ P2 alerts
    for alert in alerts:
        dim_val = alert.get("dimension_value", "")
        if alert.get("priority") == "P2" and p2_counts.get(dim_val, 0) >= 2:
            alert["priority"] = "P1"
            alert["signal_type"] = alert.get("signal_type", "") + "_COMPOUND_UPGRADE"

    return alerts


def get_thresholds(kpi_type: str) -> dict:
    """Return threshold dict for a given KPI type."""
    thresholds = {
        "SPSF": SPSF_THRESHOLDS,
        "SELL_THRU": SELL_THRU_THRESHOLDS,
        "DOI": DOI_THRESHOLDS,
        "MBQ": MBQ_THRESHOLDS,
    }
    return thresholds.get(kpi_type.upper(), {})
