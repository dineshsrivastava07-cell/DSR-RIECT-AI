"""
DSR|RIECT — MBQ Engine
Minimum Baseline Quantity compliance: SOH ≥ MBQ per SKU per store
"""

import logging
import pandas as pd
import numpy as np
from config import MBQ_THRESHOLDS

logger = logging.getLogger(__name__)


def compute_mbq(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute MBQ compliance for each row.
    Expects: store_id, article_id/sku, soh_qty, mbq
    """
    df = df.copy()
    df.columns = [c.lower().strip() for c in df.columns]

    col_aliases = {
        "stock_on_hand": "soh_qty",
        "soh": "soh_qty",
        "min_baseline_qty": "mbq",
        "minimum_qty": "mbq",
        "target_qty": "mbq",
        "sku": "article_id",
        "article": "article_id",
    }
    df.rename(columns=col_aliases, inplace=True)

    if "soh_qty" not in df.columns or "mbq" not in df.columns:
        logger.warning("MBQ: missing soh_qty or mbq columns")
        return df

    df["compliance_pct"] = (df["soh_qty"] / df["mbq"].replace(0, np.nan)).round(4)
    df["shortfall_qty"] = (df["mbq"] - df["soh_qty"]).clip(lower=0)
    df["compliant"] = df["soh_qty"] >= df["mbq"]
    df["stockout_risk"] = df["soh_qty"] == 0
    df["so_trigger"] = df["shortfall_qty"] > 0  # Suggested Order trigger

    df["priority"] = df["compliance_pct"].apply(_classify_priority)

    return df


def _classify_priority(compliance_pct) -> str:
    if pd.isna(compliance_pct):
        return "P1"  # Missing data = P1 (worst case)
    if compliance_pct < MBQ_THRESHOLDS["critical_shortfall_pct"]:
        return "P1"
    elif compliance_pct < MBQ_THRESHOLDS["high_shortfall_pct"]:
        return "P2"
    elif compliance_pct < MBQ_THRESHOLDS["medium_shortfall_pct"]:
        return "P3"
    return "P4"


def get_mbq_summary(df: pd.DataFrame) -> dict:
    if df.empty or "compliance_pct" not in df.columns:
        return {}

    priority_counts = df["priority"].value_counts().to_dict() if "priority" in df.columns else {}
    total = len(df)
    compliant_count = int(df["compliant"].sum()) if "compliant" in df.columns else 0

    return {
        "total_skus": total,
        "compliant_count": compliant_count,
        "compliance_rate_pct": round(compliant_count / max(total, 1) * 100, 1),
        "stockout_count": int(df["stockout_risk"].sum()) if "stockout_risk" in df.columns else 0,
        "so_trigger_count": int(df["so_trigger"].sum()) if "so_trigger" in df.columns else 0,
        "total_shortfall_qty": round(df["shortfall_qty"].sum(), 0) if "shortfall_qty" in df.columns else 0,
        "p1_count": priority_counts.get("P1", 0),
        "p2_count": priority_counts.get("P2", 0),
        "p3_count": priority_counts.get("P3", 0),
    }


def get_breach_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "priority" not in df.columns:
        return pd.DataFrame()
    return df[df["priority"].isin(["P1", "P2", "P3"])].sort_values("compliance_pct")
