"""
DSR|RIECT — Sell-Through Engine
Sell-Thru % — two industry-standard variants (auto-selected by available columns):
  Variant A (Primary — Indian retail standard):
    ST% = SALE_QTY ÷ (OPEN_QTY + IN_QTY)
    "Of everything received, how much sold?" — preferred for buying/planning
  Variant B (Fallback — closing stock method):
    ST% = SALE_QTY ÷ (SALE_QTY + SOH)
    Used when opening stock data is unavailable
"""

import logging
import pandas as pd
import numpy as np
from config import SELL_THRU_THRESHOLDS

logger = logging.getLogger(__name__)


def _get_thresholds() -> dict:
    """Load thresholds from RIECT-Plan (SQLite), fall back to config.py."""
    try:
        from settings.riect_plan_store import get_kpi_targets
        t = get_kpi_targets("SELL_THRU")
        if t:
            return t
    except Exception:
        pass
    return SELL_THRU_THRESHOLDS


def compute_sell_thru(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute Sell-Through % for each row — auto-selects best available method.

    Variant A (preferred): SALE_QTY / (OPEN_QTY + IN_QTY)
      Requires: net_sales_qty + opening_stock_qty + inward_qty
      Source columns from inventory_monthly_movements_opt: SALE_QTY, OPEN_QTY, IN_QTY

    Variant B (fallback): SALE_QTY / (SALE_QTY + SOH)
      Requires: net_sales_qty + closing_inventory_qty
      Source columns: SALE_QTY, SOH

    Thresholds sourced from RIECT-Plan (SQLite) if configured, else config.py defaults.
    """
    df = df.copy()
    df.columns = [c.lower().strip() for c in df.columns]

    # Alias mapping — covers inventory_monthly_movements_opt and sales table column names
    col_aliases = {
        # Sale qty aliases
        "sales_qty": "net_sales_qty",
        "sold_qty": "net_sales_qty",
        "sale_qty": "net_sales_qty",          # inventory_monthly_movements_opt
        "inv_sale_qty": "net_sales_qty",
        # Closing stock aliases (Variant B)
        "closing_inv": "closing_inventory_qty",
        "closing_stock": "closing_inventory_qty",
        "inv_qty": "closing_inventory_qty",
        "soh": "closing_inventory_qty",        # SOH = stock on hand = closing stock
        "stock_on_hand": "closing_inventory_qty",
        # Opening stock aliases (Variant A)
        "open_qty": "opening_stock_qty",       # inventory_monthly_movements_opt OPEN_QTY
        "opening_stock": "opening_stock_qty",
        "open_stock": "opening_stock_qty",
        # Inward/receipt aliases (Variant A)
        "in_qty": "inward_qty",               # inventory_monthly_movements_opt IN_QTY
        "inward": "inward_qty",
        "receipt_qty": "inward_qty",
    }
    df.rename(columns=col_aliases, inplace=True)

    # Determine which variant to use
    has_variant_a = (
        "net_sales_qty" in df.columns
        and "opening_stock_qty" in df.columns
        and "inward_qty" in df.columns
    )
    has_variant_b = (
        "net_sales_qty" in df.columns
        and "closing_inventory_qty" in df.columns
    )

    if not has_variant_a and not has_variant_b:
        logger.warning(
            "Sell-Thru: missing required columns. "
            "Need SALE_QTY + (OPEN_QTY & IN_QTY) for Variant A, "
            "or SALE_QTY + SOH for Variant B."
        )
        return df

    thresholds = _get_thresholds()

    if has_variant_a:
        # ── Variant A: Opening Stock method (Indian retail standard) ──────────
        total_available = (df["opening_stock_qty"] + df["inward_qty"]).replace(0, np.nan)
        df["sell_thru_pct"] = (df["net_sales_qty"] / total_available).round(4)
        df["sell_thru_method"] = "A: SALE_QTY/(OPEN_QTY+IN_QTY)"
        logger.info("Sell-Thru: Variant A — Opening Stock method (SALE_QTY / (OPEN_QTY + IN_QTY))")
    else:
        # ── Variant B: Closing Stock method (fallback) ────────────────────────
        total = df["net_sales_qty"] + df["closing_inventory_qty"]
        df["sell_thru_pct"] = (df["net_sales_qty"] / total.replace(0, np.nan)).round(4)
        df["sell_thru_method"] = "B: SALE_QTY/(SALE_QTY+SOH)"
        logger.info("Sell-Thru: Variant B — Closing Stock method (SALE_QTY / (SALE_QTY + SOH))")

    df["sell_thru_pct_display"] = (df["sell_thru_pct"] * 100).round(1)

    df["priority"] = df["sell_thru_pct"].apply(lambda x: _classify_priority(x, thresholds))
    df["gap_to_target"] = ((thresholds["target"] - df["sell_thru_pct"]) * 100).round(1)
    df["threshold_source"] = thresholds.get("source", "config_default")

    # Markdown trigger: if sell_thru < P2 threshold
    df["markdown_trigger"] = df["sell_thru_pct"] < thresholds["P2"]

    return df


def _classify_priority(pct, thresholds: dict) -> str:
    if pd.isna(pct):
        return "P4"
    if pct < thresholds["P1"]:
        return "P1"
    elif pct < thresholds["P2"]:
        return "P2"
    elif pct < thresholds["P3"]:
        return "P3"
    return "P4"


def get_sell_thru_summary(df: pd.DataFrame) -> dict:
    """Return summary statistics for Sell-Through analysis."""
    if df.empty or "sell_thru_pct" not in df.columns:
        return {}

    thresholds = _get_thresholds()
    priority_counts = df["priority"].value_counts().to_dict() if "priority" in df.columns else {}
    threshold_source = df["threshold_source"].iloc[0] if "threshold_source" in df.columns else "config_default"

    return {
        "avg_sell_thru_pct": round(df["sell_thru_pct"].mean() * 100, 1),
        "min_sell_thru_pct": round(df["sell_thru_pct"].min() * 100, 1),
        "max_sell_thru_pct": round(df["sell_thru_pct"].max() * 100, 1),
        "markdown_candidates": int(df["markdown_trigger"].sum()) if "markdown_trigger" in df.columns else 0,
        "p1_count": priority_counts.get("P1", 0),
        "p2_count": priority_counts.get("P2", 0),
        "p3_count": priority_counts.get("P3", 0),
        "total_categories": len(df),
        "target_pct": round(thresholds["target"] * 100, 1),
        "threshold_source": threshold_source,
    }


def get_breach_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "priority" not in df.columns:
        return pd.DataFrame()
    return df[df["priority"].isin(["P1", "P2", "P3"])].sort_values("sell_thru_pct")
