"""
DSR|RIECT — DOI Engine
Days of Inventory = (Stock on Hand + Goods in Transit) ÷ Average Daily Sales
"""

import logging
import pandas as pd
import numpy as np
from config import DOI_THRESHOLDS

logger = logging.getLogger(__name__)


def compute_doi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute DOI for each row.
    Expects: store_id/article_id, soh_qty, git_qty, avg_daily_sales
    """
    df = df.copy()
    df.columns = [c.lower().strip() for c in df.columns]

    # Alias mapping
    col_aliases = {
        "stock_on_hand": "soh_qty",
        "soh": "soh_qty",
        "goods_in_transit": "git_qty",
        "git": "git_qty",
        "in_transit": "git_qty",
        "avg_daily_sale": "avg_daily_sales",
        "daily_sales": "avg_daily_sales",
    }
    df.rename(columns=col_aliases, inplace=True)

    # Default git_qty = 0 if not present
    if "git_qty" not in df.columns:
        df["git_qty"] = 0
    if "soh_qty" not in df.columns:
        logger.warning("DOI: missing soh_qty column")
        return df
    if "avg_daily_sales" not in df.columns:
        logger.warning("DOI: missing avg_daily_sales column")
        return df

    total_inventory = df["soh_qty"] + df["git_qty"]
    df["doi"] = (total_inventory / df["avg_daily_sales"].replace(0, np.nan)).round(1)
    df["days_of_cover"] = df["doi"]  # alias

    df["priority"] = df["doi"].apply(_classify_priority)
    df["overstock_risk"] = df["doi"] > DOI_THRESHOLDS["P1"]
    df["gap_to_target"] = (df["doi"] - DOI_THRESHOLDS["target"]).round(1)

    return df


def _classify_priority(doi) -> str:
    if pd.isna(doi):
        return "P4"
    if doi > DOI_THRESHOLDS["P1"]:
        return "P1"
    elif doi > DOI_THRESHOLDS["P2"]:
        return "P2"
    elif doi > DOI_THRESHOLDS["P3"]:
        return "P3"
    return "P4"


def get_doi_summary(df: pd.DataFrame) -> dict:
    if df.empty or "doi" not in df.columns:
        return {}

    priority_counts = df["priority"].value_counts().to_dict() if "priority" in df.columns else {}

    return {
        "avg_doi": round(df["doi"].mean(), 1),
        "max_doi": round(df["doi"].max(), 1),
        "min_doi": round(df["doi"].min(), 1),
        "overstock_count": int(df["overstock_risk"].sum()) if "overstock_risk" in df.columns else 0,
        "p1_count": priority_counts.get("P1", 0),
        "p2_count": priority_counts.get("P2", 0),
        "p3_count": priority_counts.get("P3", 0),
        "total_skus": len(df),
        "target_doi": DOI_THRESHOLDS["target"],
    }


def get_breach_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "priority" not in df.columns:
        return pd.DataFrame()
    return df[df["priority"].isin(["P1", "P2", "P3"])].sort_values("doi", ascending=False)
