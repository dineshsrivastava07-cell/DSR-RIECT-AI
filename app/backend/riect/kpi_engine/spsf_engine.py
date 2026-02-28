"""
DSR|RIECT — SPSF Engine
Sales Per Square Foot = Net Sales Amount ÷ Floor SqFt
Operates on DataFrames from ClickHouse query results
"""

import logging
import pandas as pd
import numpy as np
from config import SPSF_THRESHOLDS

logger = logging.getLogger(__name__)

REQUIRED_COLS = ["store_id", "net_sales_amount", "floor_sqft"]
OPTIONAL_COLS = ["store_name", "date", "period"]

# Sentinel added to df when floor_sqft is unavailable
SPSF_UNAVAILABLE_FLAG = "floor_sqft_unavailable"


def compute_spsf(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute SPSF for each row.
    Expects: store_id, net_sales_amount, floor_sqft
    Returns: DataFrame with spsf, priority, gap columns added.
    If floor_sqft is missing, adds spsf_status='floor_sqft_unavailable' and returns
    without computing SPSF — caller/LLM must not invent floor area values.
    """
    df = df.copy()

    # Normalise column names
    df.columns = [c.lower().strip() for c in df.columns]

    # Map common alias columns
    col_aliases = {
        "sales_amount": "net_sales_amount",
        "total_sales": "net_sales_amount",
        "netamt": "net_sales_amount",
        "total_netamt": "net_sales_amount",
        "total_net_sales": "net_sales_amount",
        "net_sales": "net_sales_amount",
        "sqft": "floor_sqft",
        "sq_ft": "floor_sqft",
        "floor_area": "floor_sqft",
    }
    df.rename(columns=col_aliases, inplace=True)

    # If spsf was pre-computed by the pipeline enrichment stage, use it directly
    if "spsf" in df.columns and df["spsf"].notna().any():
        logger.debug("SPSF: using pre-computed spsf values from pipeline enrichment")
        df["priority"] = df["spsf"].apply(_classify_priority)
        df["gap_to_p1"] = (SPSF_THRESHOLDS["P1"] - df["spsf"]).clip(lower=0).round(2)
        df["gap_to_target"] = (SPSF_THRESHOLDS["target"] - df["spsf"]).round(2)
        return df

    if "net_sales_amount" not in df.columns or "floor_sqft" not in df.columns:
        logger.warning(
            "SPSF: floor_sqft column not found in query result. "
            "Store floor area data is NOT in ClickHouse. "
            "SPSF cannot be computed. Do NOT estimate or invent sqft values."
        )
        df["spsf"] = None
        df["spsf_status"] = SPSF_UNAVAILABLE_FLAG
        return df

    # Compute SPSF from raw columns
    df["spsf"] = df["net_sales_amount"] / df["floor_sqft"].replace(0, np.nan)
    df["spsf"] = df["spsf"].round(2)

    # Classify priority
    df["priority"] = df["spsf"].apply(_classify_priority)
    df["gap_to_p1"] = (SPSF_THRESHOLDS["P1"] - df["spsf"]).clip(lower=0).round(2)
    df["gap_to_target"] = (SPSF_THRESHOLDS["target"] - df["spsf"]).round(2)

    return df


def _classify_priority(spsf_value) -> str:
    if pd.isna(spsf_value):
        return "P4"
    if spsf_value < SPSF_THRESHOLDS["P1"]:
        return "P1"
    elif spsf_value < SPSF_THRESHOLDS["P2"]:
        return "P2"
    elif spsf_value < SPSF_THRESHOLDS["P3"]:
        return "P3"
    return "P4"


def get_spsf_summary(df: pd.DataFrame) -> dict:
    """Return summary statistics for SPSF analysis."""
    if df.empty:
        return {}
    # Check for unavailable flag — floor_sqft not in ClickHouse
    if "spsf_status" in df.columns and (df["spsf_status"] == SPSF_UNAVAILABLE_FLAG).any():
        return {
            "spsf_available": False,
            "reason": "Store floor area (sqft) data is not present in ClickHouse. "
                      "Upload a store master table with sqft to enable SPSF calculation.",
        }
    if "spsf" not in df.columns:
        return {}

    priority_counts = df["priority"].value_counts().to_dict() if "priority" in df.columns else {}

    return {
        "avg_spsf": round(df["spsf"].mean(), 2),
        "min_spsf": round(df["spsf"].min(), 2),
        "max_spsf": round(df["spsf"].max(), 2),
        "p1_count": priority_counts.get("P1", 0),
        "p2_count": priority_counts.get("P2", 0),
        "p3_count": priority_counts.get("P3", 0),
        "p4_count": priority_counts.get("P4", 0),
        "total_stores": len(df),
        "target_spsf": SPSF_THRESHOLDS["target"],
    }


def get_breach_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Return only rows that breach a threshold (P1, P2, or P3)."""
    if "priority" not in df.columns:
        return pd.DataFrame()
    return df[df["priority"].isin(["P1", "P2", "P3"])].sort_values("spsf")
