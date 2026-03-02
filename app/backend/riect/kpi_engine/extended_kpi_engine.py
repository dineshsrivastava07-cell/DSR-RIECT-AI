"""
DSR|RIECT — Extended KPI Engine
Basket, Margin, Customer, Store Operations, Inventory Extended,
Procurement, and Planning KPI engines.

All engines follow the pattern:
  compute_<kpi>(df)          → df_with_kpi_col (raises ValueError if required cols missing)
  get_<kpi>_summary(df)      → dict (empty dict if kpi col absent)
  get_<kpi>_breach_rows(df)  → pd.DataFrame (empty if no breaches)

All column aliases are lowercase (kpi_controller normalises df.columns).
"""

import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ─── Column alias groups (lowercase) ──────────────────────────────────────────

SALES_COLS  = ["netamt", "net_sales", "totalsales", "total_sales", "netsales", "net_sales_amount"]
BILL_COLS   = ["bill_count", "bills_count", "transaction_count", "billno_count", "bills", "txn_count"]
QTY_COLS    = ["qty", "total_qty", "units_sold", "sale_qty"]
GROSS_COLS  = ["grossamt", "gross_amt"]
DISC_COLS   = ["discountamt", "discount_amt"]
PROMO_COLS  = ["promoamt", "promo_amt"]
COST_COLS   = ["cost_price", "cost_price_total", "cogs", "cost_of_goods"]
SOH_COLS    = ["soh", "as_on_stk", "total_stock", "total_soh"]
GIT_COLS    = ["git", "in_transit", "goods_in_transit"]
MBQ_COLS    = ["mbq", "min_baseline_qty"]
AOP_COLS    = ["aop_target", "plan_sales", "target_sales", "aop"]
CUST_COLS   = ["mobile_no", "cust_id", "customer_id", "mobile", "customer_mobile"]
UCUST_COLS  = ["unique_customers"]   # pre-aggregated customer count


# ─── Shared helpers ────────────────────────────────────────────────────────────

def _first_col(df: pd.DataFrame, candidates: list):
    """Return the first candidate column name present in df, or None."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _priority_label(value: float, thresholds: dict, direction: str = "low") -> str:
    """
    Assign P1/P2/P3/P4 based on thresholds dict with keys P1, P2, P3.
    direction="low"  → bad when value is LOW (e.g. ATV, UPT, Margin): compare value < threshold
    direction="high" → bad when value is HIGH (e.g. discount rate): compare value > threshold
    """
    p1, p2, p3 = thresholds["P1"], thresholds["P2"], thresholds["P3"]
    if direction == "low":
        if value < p1: return "P1"
        if value < p2: return "P2"
        if value < p3: return "P3"
    else:  # high
        if value > p1: return "P1"
        if value > p2: return "P2"
        if value > p3: return "P3"
    return "P4"


# ══════════════════════════════════════════════════════════════════════════════
# 3A — BASKET ENGINE: ATV + UPT
# ══════════════════════════════════════════════════════════════════════════════

def compute_atv(df: pd.DataFrame) -> pd.DataFrame:
    s = _first_col(df, SALES_COLS)
    b = _first_col(df, BILL_COLS)
    if not s or not b:
        raise ValueError(f"ATV: missing sales col ({s}) or bill col ({b})")
    df = df.copy()
    df["atv"] = _to_num(df[s]) / _to_num(df[b]).replace(0, np.nan)
    return df


def get_atv_summary(df: pd.DataFrame) -> dict:
    if "atv" not in df.columns:
        return {}
    from config import ATV_THRESHOLDS
    atv = _to_num(df["atv"]).dropna()
    if atv.empty:
        return {}
    p1 = int((atv < ATV_THRESHOLDS["P1"]).sum())
    p2 = int(((atv >= ATV_THRESHOLDS["P1"]) & (atv < ATV_THRESHOLDS["P2"])).sum())
    p3 = int(((atv >= ATV_THRESHOLDS["P2"]) & (atv < ATV_THRESHOLDS["P3"])).sum())
    return {
        "mean_atv":   round(float(atv.mean()), 2),
        "median_atv": round(float(atv.median()), 2),
        "min_atv":    round(float(atv.min()), 2),
        "max_atv":    round(float(atv.max()), 2),
        "p1_count": p1, "p2_count": p2, "p3_count": p3,
        "target": ATV_THRESHOLDS["target"],
    }


def get_atv_breach_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "atv" not in df.columns:
        return pd.DataFrame()
    from config import ATV_THRESHOLDS
    atv = _to_num(df["atv"])
    breaches = df[atv < ATV_THRESHOLDS["P3"]].copy()
    if breaches.empty:
        return pd.DataFrame()
    breaches["priority"] = atv[atv < ATV_THRESHOLDS["P3"]].apply(
        lambda v: _priority_label(v, ATV_THRESHOLDS, "low")
    )
    return breaches[breaches["priority"] != "P4"].sort_values("atv")


def compute_upt(df: pd.DataFrame) -> pd.DataFrame:
    q = _first_col(df, QTY_COLS)
    b = _first_col(df, BILL_COLS)
    if not q or not b:
        raise ValueError(f"UPT: missing qty col ({q}) or bill col ({b})")
    df = df.copy()
    df["upt"] = _to_num(df[q]) / _to_num(df[b]).replace(0, np.nan)
    return df


def get_upt_summary(df: pd.DataFrame) -> dict:
    if "upt" not in df.columns:
        return {}
    from config import UPT_THRESHOLDS
    upt = _to_num(df["upt"]).dropna()
    if upt.empty:
        return {}
    p1 = int((upt < UPT_THRESHOLDS["P1"]).sum())
    p2 = int(((upt >= UPT_THRESHOLDS["P1"]) & (upt < UPT_THRESHOLDS["P2"])).sum())
    p3 = int(((upt >= UPT_THRESHOLDS["P2"]) & (upt < UPT_THRESHOLDS["P3"])).sum())
    return {
        "mean_upt":   round(float(upt.mean()), 2),
        "median_upt": round(float(upt.median()), 2),
        "min_upt":    round(float(upt.min()), 2),
        "max_upt":    round(float(upt.max()), 2),
        "p1_count": p1, "p2_count": p2, "p3_count": p3,
        "target": UPT_THRESHOLDS["target"],
    }


def get_upt_breach_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "upt" not in df.columns:
        return pd.DataFrame()
    from config import UPT_THRESHOLDS
    upt = _to_num(df["upt"])
    breaches = df[upt < UPT_THRESHOLDS["P3"]].copy()
    if breaches.empty:
        return pd.DataFrame()
    breaches["priority"] = upt[upt < UPT_THRESHOLDS["P3"]].apply(
        lambda v: _priority_label(v, UPT_THRESHOLDS, "low")
    )
    return breaches[breaches["priority"] != "P4"].sort_values("upt")


# ══════════════════════════════════════════════════════════════════════════════
# 3B — MARGIN ENGINE: Discount Rate, Non-Promo Discount, Gross Margin
# ══════════════════════════════════════════════════════════════════════════════

def compute_discount_rate(df: pd.DataFrame) -> pd.DataFrame:
    d = _first_col(df, DISC_COLS)
    g = _first_col(df, GROSS_COLS)
    if not d or not g:
        raise ValueError(f"Discount Rate: missing disc col ({d}) or gross col ({g})")
    df = df.copy()
    gross = _to_num(df[g]).replace(0, np.nan)
    df["discount_rate"] = _to_num(df[d]) / gross
    return df


def get_discount_rate_summary(df: pd.DataFrame) -> dict:
    if "discount_rate" not in df.columns:
        return {}
    from config import DISCOUNT_RATE_THRESHOLDS
    dr = _to_num(df["discount_rate"]).dropna()
    if dr.empty:
        return {}
    p1 = int((dr > DISCOUNT_RATE_THRESHOLDS["P1"]).sum())
    p2 = int(((dr <= DISCOUNT_RATE_THRESHOLDS["P1"]) & (dr > DISCOUNT_RATE_THRESHOLDS["P2"])).sum())
    p3 = int(((dr <= DISCOUNT_RATE_THRESHOLDS["P2"]) & (dr > DISCOUNT_RATE_THRESHOLDS["P3"])).sum())
    return {
        "mean_discount_rate":   round(float(dr.mean()), 4),
        "median_discount_rate": round(float(dr.median()), 4),
        "max_discount_rate":    round(float(dr.max()), 4),
        "p1_count": p1, "p2_count": p2, "p3_count": p3,
        "target": DISCOUNT_RATE_THRESHOLDS["target"],
    }


def get_discount_rate_breach_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "discount_rate" not in df.columns:
        return pd.DataFrame()
    from config import DISCOUNT_RATE_THRESHOLDS
    dr = _to_num(df["discount_rate"])
    breaches = df[dr > DISCOUNT_RATE_THRESHOLDS["P3"]].copy()
    if breaches.empty:
        return pd.DataFrame()
    breaches["priority"] = dr[dr > DISCOUNT_RATE_THRESHOLDS["P3"]].apply(
        lambda v: _priority_label(v, DISCOUNT_RATE_THRESHOLDS, "high")
    )
    return breaches[breaches["priority"] != "P4"].sort_values("discount_rate", ascending=False)


def compute_non_promo_disc(df: pd.DataFrame) -> pd.DataFrame:
    d = _first_col(df, DISC_COLS)
    p = _first_col(df, PROMO_COLS)
    g = _first_col(df, GROSS_COLS)
    if not d or not p or not g:
        raise ValueError(f"Non-Promo Disc: missing disc ({d}), promo ({p}), or gross ({g}) col")
    df = df.copy()
    gross = _to_num(df[g]).replace(0, np.nan)
    non_promo = (_to_num(df[d]) - _to_num(df[p])).clip(lower=0)
    df["non_promo_disc_rate"] = non_promo / gross
    return df


def get_non_promo_disc_summary(df: pd.DataFrame) -> dict:
    if "non_promo_disc_rate" not in df.columns:
        return {}
    from config import NON_PROMO_DISC_THRESHOLDS
    npd = _to_num(df["non_promo_disc_rate"]).dropna()
    if npd.empty:
        return {}
    p1 = int((npd > NON_PROMO_DISC_THRESHOLDS["P1"]).sum())
    p2 = int(((npd <= NON_PROMO_DISC_THRESHOLDS["P1"]) & (npd > NON_PROMO_DISC_THRESHOLDS["P2"])).sum())
    p3 = int(((npd <= NON_PROMO_DISC_THRESHOLDS["P2"]) & (npd > NON_PROMO_DISC_THRESHOLDS["P3"])).sum())
    return {
        "mean_non_promo_disc":   round(float(npd.mean()), 4),
        "max_non_promo_disc":    round(float(npd.max()), 4),
        "p1_count": p1, "p2_count": p2, "p3_count": p3,
        "target": NON_PROMO_DISC_THRESHOLDS["target"],
    }


def get_non_promo_disc_breach_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "non_promo_disc_rate" not in df.columns:
        return pd.DataFrame()
    from config import NON_PROMO_DISC_THRESHOLDS
    npd = _to_num(df["non_promo_disc_rate"])
    breaches = df[npd > NON_PROMO_DISC_THRESHOLDS["P3"]].copy()
    if breaches.empty:
        return pd.DataFrame()
    breaches["priority"] = npd[npd > NON_PROMO_DISC_THRESHOLDS["P3"]].apply(
        lambda v: _priority_label(v, NON_PROMO_DISC_THRESHOLDS, "high")
    )
    return breaches[breaches["priority"] != "P4"].sort_values("non_promo_disc_rate", ascending=False)


def compute_gross_margin(df: pd.DataFrame) -> pd.DataFrame:
    s = _first_col(df, SALES_COLS)
    c = _first_col(df, COST_COLS)
    if not s or not c:
        raise ValueError(f"Gross Margin: missing sales col ({s}) or cost col ({c})")
    df = df.copy()
    net = _to_num(df[s]).replace(0, np.nan)
    df["gross_margin_pct"] = (net - _to_num(df[c])) / net
    return df


def get_gross_margin_summary(df: pd.DataFrame) -> dict:
    if "gross_margin_pct" not in df.columns:
        return {}
    from config import GROSS_MARGIN_THRESHOLDS
    gm = _to_num(df["gross_margin_pct"]).dropna()
    if gm.empty:
        return {}
    p1 = int((gm < GROSS_MARGIN_THRESHOLDS["P1"]).sum())
    p2 = int(((gm >= GROSS_MARGIN_THRESHOLDS["P1"]) & (gm < GROSS_MARGIN_THRESHOLDS["P2"])).sum())
    p3 = int(((gm >= GROSS_MARGIN_THRESHOLDS["P2"]) & (gm < GROSS_MARGIN_THRESHOLDS["P3"])).sum())
    return {
        "mean_gross_margin":   round(float(gm.mean()), 4),
        "median_gross_margin": round(float(gm.median()), 4),
        "min_gross_margin":    round(float(gm.min()), 4),
        "p1_count": p1, "p2_count": p2, "p3_count": p3,
        "target": GROSS_MARGIN_THRESHOLDS["target"],
    }


def get_gross_margin_breach_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "gross_margin_pct" not in df.columns:
        return pd.DataFrame()
    from config import GROSS_MARGIN_THRESHOLDS
    gm = _to_num(df["gross_margin_pct"])
    breaches = df[gm < GROSS_MARGIN_THRESHOLDS["P3"]].copy()
    if breaches.empty:
        return pd.DataFrame()
    breaches["priority"] = gm[gm < GROSS_MARGIN_THRESHOLDS["P3"]].apply(
        lambda v: _priority_label(v, GROSS_MARGIN_THRESHOLDS, "low")
    )
    return breaches[breaches["priority"] != "P4"].sort_values("gross_margin_pct")


# ══════════════════════════════════════════════════════════════════════════════
# 3C — CUSTOMER ENGINE: Unique Customers, Mobile Penetration
# ══════════════════════════════════════════════════════════════════════════════

def compute_mobile_pct(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute mobile penetration %.
    Handles two scenarios:
      1. Pre-aggregated: unique_customers column already present (use directly).
      2. Raw: mobile_no / customer_mobile column present (derive unique count per row — for
         row-level data, unique count = 1 per distinct customer — not meaningful at this level;
         mobile_pct is only meaningful when data is already aggregated by store).
    In both cases, bill_count is required.
    """
    b = _first_col(df, BILL_COLS)
    if not b:
        raise ValueError("Mobile Pct: missing bill count column")

    df = df.copy()
    bills = _to_num(df[b]).replace(0, np.nan)

    # Prefer pre-aggregated unique_customers column
    uc_col = _first_col(df, UCUST_COLS)
    if uc_col:
        df["mobile_pct"] = _to_num(df[uc_col]) / bills
    else:
        # If raw customer col present — flag 1 unique customer per row (aggregated data assumed)
        cust_col = _first_col(df, CUST_COLS)
        if not cust_col:
            raise ValueError("Mobile Pct: no unique_customers or mobile_no column found")
        # For aggregated store-level data, unique_customers must be pre-computed
        # Fallback: 1.0 penetration per row (data may need re-aggregation at SQL level)
        logger.debug(f"Mobile Pct: using raw col '{cust_col}' — assuming pre-aggregated counts")
        df["mobile_pct"] = _to_num(df[cust_col]) / bills

    return df


def get_mobile_pct_summary(df: pd.DataFrame) -> dict:
    if "mobile_pct" not in df.columns:
        return {}
    from config import MOBILE_PENETRATION_THRESHOLDS
    mp = _to_num(df["mobile_pct"]).dropna()
    if mp.empty:
        return {}
    p1 = int((mp < MOBILE_PENETRATION_THRESHOLDS["P1"]).sum())
    p2 = int(((mp >= MOBILE_PENETRATION_THRESHOLDS["P1"]) & (mp < MOBILE_PENETRATION_THRESHOLDS["P2"])).sum())
    p3 = int(((mp >= MOBILE_PENETRATION_THRESHOLDS["P2"]) & (mp < MOBILE_PENETRATION_THRESHOLDS["P3"])).sum())
    return {
        "mean_mobile_pct":   round(float(mp.mean()), 4),
        "median_mobile_pct": round(float(mp.median()), 4),
        "min_mobile_pct":    round(float(mp.min()), 4),
        "max_mobile_pct":    round(float(mp.max()), 4),
        "p1_count": p1, "p2_count": p2, "p3_count": p3,
        "target": MOBILE_PENETRATION_THRESHOLDS["target"],
    }


def get_mobile_pct_breach_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "mobile_pct" not in df.columns:
        return pd.DataFrame()
    from config import MOBILE_PENETRATION_THRESHOLDS
    mp = _to_num(df["mobile_pct"])
    breaches = df[mp < MOBILE_PENETRATION_THRESHOLDS["P3"]].copy()
    if breaches.empty:
        return pd.DataFrame()
    breaches["priority"] = mp[mp < MOBILE_PENETRATION_THRESHOLDS["P3"]].apply(
        lambda v: _priority_label(v, MOBILE_PENETRATION_THRESHOLDS, "low")
    )
    return breaches[breaches["priority"] != "P4"].sort_values("mobile_pct")


# ══════════════════════════════════════════════════════════════════════════════
# 3D — STORE OPERATIONS ENGINE: Bill Integrity
# ══════════════════════════════════════════════════════════════════════════════

def compute_bill_integrity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Bill Integrity = NETAMT / (GROSSAMT - DISCOUNTAMT)
    PROMOAMT is already included in DISCOUNTAMT — no separate subtraction.
    """
    n = _first_col(df, SALES_COLS + ["netamt"])
    g = _first_col(df, GROSS_COLS)
    d = _first_col(df, DISC_COLS)
    if not n or not g or not d:
        raise ValueError(f"Bill Integrity: missing netamt ({n}), grossamt ({g}), or discountamt ({d})")
    df = df.copy()
    expected_net = (_to_num(df[g]) - _to_num(df[d])).replace(0, np.nan)
    df["bill_integrity"] = _to_num(df[n]) / expected_net
    return df


def get_bill_integrity_summary(df: pd.DataFrame) -> dict:
    if "bill_integrity" not in df.columns:
        return {}
    from config import BILL_INTEGRITY_THRESHOLDS
    bi = _to_num(df["bill_integrity"]).dropna()
    # Only consider rows where net_amt >= 0 (exclude pure return rows)
    bi = bi[bi >= 0]
    if bi.empty:
        return {}
    p1 = int((bi < BILL_INTEGRITY_THRESHOLDS["P1"]).sum())
    p2 = int(((bi >= BILL_INTEGRITY_THRESHOLDS["P1"]) & (bi < BILL_INTEGRITY_THRESHOLDS["P2"])).sum())
    p3 = int(((bi >= BILL_INTEGRITY_THRESHOLDS["P2"]) & (bi < BILL_INTEGRITY_THRESHOLDS["P3"])).sum())
    return {
        "mean_bill_integrity":   round(float(bi.mean()), 4),
        "min_bill_integrity":    round(float(bi.min()), 4),
        "p1_count": p1, "p2_count": p2, "p3_count": p3,
        "target": BILL_INTEGRITY_THRESHOLDS["target"],
    }


def get_bill_integrity_breach_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "bill_integrity" not in df.columns:
        return pd.DataFrame()
    from config import BILL_INTEGRITY_THRESHOLDS
    bi = _to_num(df["bill_integrity"])
    mask = (bi < BILL_INTEGRITY_THRESHOLDS["P3"]) & (bi >= 0)
    breaches = df[mask].copy()
    if breaches.empty:
        return pd.DataFrame()
    breaches["priority"] = bi[mask].apply(
        lambda v: _priority_label(v, BILL_INTEGRITY_THRESHOLDS, "low")
    )
    return breaches[breaches["priority"] != "P4"].sort_values("bill_integrity")


# ══════════════════════════════════════════════════════════════════════════════
# 3E — INVENTORY EXTENDED ENGINE: SOH Health, GIT Coverage
# ══════════════════════════════════════════════════════════════════════════════

def compute_soh_health(df: pd.DataFrame) -> pd.DataFrame:
    """
    Classify each row's SOH health:
      stockout:  SOH == 0
      at_risk:   SOH > 0 and SOH < 50% of MBQ (when mbq column present)
      overstock: DOI > DOI_THRESHOLDS["P1"] (when doi column present)
      healthy:   otherwise
    """
    soh_col = _first_col(df, SOH_COLS)
    if not soh_col:
        raise ValueError("SOH Health: no SOH column found")
    from config import DOI_THRESHOLDS, MBQ_THRESHOLDS
    df = df.copy()
    soh = _to_num(df[soh_col])

    mbq_col = _first_col(df, MBQ_COLS)
    doi_col = next((c for c in ["doi", "doi_days", "days_of_inventory"] if c in df.columns), None)

    def _classify(idx):
        s = soh.loc[idx]
        if pd.isna(s):
            return "unknown"
        if s == 0:
            return "stockout"
        if mbq_col:
            mbq_val = _to_num(df[mbq_col]).loc[idx]
            if not pd.isna(mbq_val) and mbq_val > 0 and s < mbq_val * MBQ_THRESHOLDS["critical_shortfall_pct"]:
                return "at_risk"
        if doi_col:
            doi_val = _to_num(df[doi_col]).loc[idx]
            if not pd.isna(doi_val) and doi_val > DOI_THRESHOLDS["P1"]:
                return "overstock"
        return "healthy"

    df["soh_health"] = [_classify(i) for i in df.index]
    return df


def get_soh_health_summary(df: pd.DataFrame) -> dict:
    if "soh_health" not in df.columns:
        return {}
    counts = df["soh_health"].value_counts().to_dict()
    return {
        "stockout_count":  int(counts.get("stockout", 0)),
        "at_risk_count":   int(counts.get("at_risk", 0)),
        "overstock_count": int(counts.get("overstock", 0)),
        "healthy_count":   int(counts.get("healthy", 0)),
        "unknown_count":   int(counts.get("unknown", 0)),
        "total_rows":      len(df),
    }


# ── GIT Coverage ──────────────────────────────────────────────────────────────

def compute_git_coverage(df: pd.DataFrame) -> pd.DataFrame:
    git_col = _first_col(df, GIT_COLS)
    if not git_col:
        raise ValueError("GIT Coverage: no GIT column found")

    df = df.copy()
    git = _to_num(df[git_col])

    # Avg daily sales proxy — prefer net_sales columns; fallback to SOH/30
    sales_col = _first_col(df, SALES_COLS)
    soh_col   = _first_col(df, SOH_COLS)

    if sales_col:
        avg_daily = _to_num(df[sales_col]) / 30
    elif soh_col:
        avg_daily = _to_num(df[soh_col]) / 30
    else:
        raise ValueError("GIT Coverage: no sales or SOH column to compute avg daily proxy")

    avg_daily = avg_daily.replace(0, np.nan)
    df["git_coverage_days"] = git / avg_daily
    return df


def get_git_coverage_summary(df: pd.DataFrame) -> dict:
    if "git_coverage_days" not in df.columns:
        return {}
    gc = _to_num(df["git_coverage_days"]).dropna()
    if gc.empty:
        return {}
    return {
        "mean_git_coverage_days":   round(float(gc.mean()), 1),
        "median_git_coverage_days": round(float(gc.median()), 1),
        "max_git_coverage_days":    round(float(gc.max()), 1),
        "total_rows": len(df),
    }


# ══════════════════════════════════════════════════════════════════════════════
# 3F — PROCUREMENT ENGINE: MBQ Shortfall Amount
# ══════════════════════════════════════════════════════════════════════════════

def compute_mbq_shortfall_amt(df: pd.DataFrame) -> pd.DataFrame:
    mbq_col = _first_col(df, MBQ_COLS)
    soh_col = _first_col(df, SOH_COLS)
    if not mbq_col or not soh_col:
        raise ValueError(f"MBQ Shortfall Amt: missing mbq col ({mbq_col}) or soh col ({soh_col})")

    df = df.copy()
    mbq = _to_num(df[mbq_col])
    soh = _to_num(df[soh_col])
    df["mbq_shortfall_qty"] = (mbq - soh).clip(lower=0)

    cost_col = _first_col(df, COST_COLS)
    if cost_col:
        df["mbq_shortfall_amt"] = df["mbq_shortfall_qty"] * _to_num(df[cost_col])
    else:
        df["mbq_shortfall_amt"] = np.nan   # amount unavailable without cost

    return df


def get_mbq_shortfall_amt_summary(df: pd.DataFrame) -> dict:
    if "mbq_shortfall_qty" not in df.columns:
        return {}
    sq = _to_num(df["mbq_shortfall_qty"]).dropna()
    if sq.empty:
        return {}
    summary = {
        "total_shortfall_qty":  int(sq.sum()),
        "stores_with_shortfall": int((sq > 0).sum()),
        "max_shortfall_qty":    int(sq.max()),
    }
    if "mbq_shortfall_amt" in df.columns:
        sa = _to_num(df["mbq_shortfall_amt"]).dropna()
        if not sa.empty:
            summary["total_shortfall_amt"] = round(float(sa.sum()), 2)
            summary["max_shortfall_amt"]   = round(float(sa.max()), 2)
    return summary


def get_mbq_shortfall_amt_breach_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "mbq_shortfall_qty" not in df.columns:
        return pd.DataFrame()
    sq = _to_num(df["mbq_shortfall_qty"])
    breaches = df[sq > 0].copy()
    if breaches.empty:
        return pd.DataFrame()
    # Sort by shortfall amount if available, else by qty
    sort_col = "mbq_shortfall_amt" if "mbq_shortfall_amt" in breaches.columns else "mbq_shortfall_qty"
    breaches["priority"] = "P2"  # All MBQ shortfalls are at minimum P2
    try:
        breaches = breaches.sort_values(sort_col, ascending=False)
    except Exception:
        pass
    return breaches


# ══════════════════════════════════════════════════════════════════════════════
# 3G — PLANNING ENGINE: AOP vs Actual
# ══════════════════════════════════════════════════════════════════════════════

def compute_aop_vs_actual(df: pd.DataFrame) -> pd.DataFrame:
    aop_col   = _first_col(df, AOP_COLS)
    sales_col = _first_col(df, SALES_COLS)
    if not aop_col or not sales_col:
        raise ValueError(f"AOP vs Actual: missing aop col ({aop_col}) or sales col ({sales_col})")

    df = df.copy()
    aop    = _to_num(df[aop_col]).replace(0, np.nan)
    actual = _to_num(df[sales_col])
    df["aop_vs_actual_pct"] = (actual - aop) / aop * 100  # +ve = over-plan, -ve = under-plan
    return df


def get_aop_summary(df: pd.DataFrame) -> dict:
    if "aop_vs_actual_pct" not in df.columns:
        return {}
    av = _to_num(df["aop_vs_actual_pct"]).dropna()
    if av.empty:
        return {}
    # Under-plan buckets: P1 < -20%, P2 < -10%, P3 < 0%
    p1 = int((av < -20).sum())
    p2 = int(((av >= -20) & (av < -10)).sum())
    p3 = int(((av >= -10) & (av < 0)).sum())
    on_plan = int((av >= 0).sum())
    return {
        "mean_aop_vs_actual_pct":   round(float(av.mean()), 2),
        "median_aop_vs_actual_pct": round(float(av.median()), 2),
        "min_aop_vs_actual_pct":    round(float(av.min()), 2),
        "max_aop_vs_actual_pct":    round(float(av.max()), 2),
        "p1_count": p1, "p2_count": p2, "p3_count": p3,
        "on_plan_count": on_plan,
    }


def get_aop_breach_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "aop_vs_actual_pct" not in df.columns:
        return pd.DataFrame()
    av = _to_num(df["aop_vs_actual_pct"])
    # Only under-plan rows are breaches
    breaches = df[av < 0].copy()
    if breaches.empty:
        return pd.DataFrame()

    def _aop_priority(v):
        if v < -20: return "P1"
        if v < -10: return "P2"
        return "P3"

    breaches["priority"] = av[av < 0].apply(_aop_priority)
    return breaches.sort_values("aop_vs_actual_pct")
