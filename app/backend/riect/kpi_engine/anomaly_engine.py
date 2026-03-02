"""
DSR|RIECT — Anomaly Detection Engine
Statistical Z-score anomaly detection across SPSF, Sell-Through, UPT, DOI.
Also detects pilferage signals, bill integrity breaches, discount anomalies, sales returns.
All detections are cross-validated against multiple columns for authenticity.
"""

import logging
import numpy as np
import pandas as pd
from typing import Optional

logger = logging.getLogger(__name__)

# Z-score threshold — values beyond ±2σ are anomalies
Z_THRESHOLD = 2.0

# Columns to check: (kpi_tag, kpi_label, bad_direction)
# bad_direction:
#   "low"  — high value is GOOD (SPSF, UPT, Sales, Sell-Through) → only flag underperformers (z < -threshold)
#   "high" — low value is GOOD (DOI) → only flag excess-inventory stores (z > +threshold)
#   "both" — flag outliers in both directions (SOH, generic)
KPI_ANOMALY_COLUMNS = {
    # SPSF — higher is better → only flag low outliers
    "spsf": ("SPSF", "Sales Per Sq Ft", "low"),
    # Sell-Through — higher is better → only flag low outliers
    "sell_thru_pct":    ("SELL_THRU", "Sell-Through %", "low"),
    "sell_through_pct": ("SELL_THRU", "Sell-Through %", "low"),
    # UPT — higher is better → only flag low outliers
    "upt":                  ("UPT", "Units Per Transaction", "low"),
    "units_per_transaction": ("UPT", "Units Per Transaction", "low"),
    # DOI — lower is better → only flag high outliers (excess stock)
    "doi":              ("DOI", "Days of Inventory", "high"),
    "days_of_inventory": ("DOI", "Days of Inventory", "high"),
    # Revenue / Sales — higher is better → only flag low outliers
    "totalsales":  ("SALES", "Total Sales", "low"),
    "total_sales": ("SALES", "Total Sales", "low"),
    "netsales":    ("SALES", "Net Sales",   "low"),
    "net_sales":   ("SALES", "Net Sales",   "low"),
    # Stock — flag both (too high = overstock, too low = stock-out risk)
    "soh":         ("SOH", "Stock on Hand", "both"),
    "total_stock": ("SOH", "Stock on Hand", "both"),
    "as_on_stk":   ("SOH", "Stock on Hand", "both"),
    # ATV — higher is better → only flag low outliers
    "atv": ("ATV", "Avg Transaction Value", "low"),
    # Discount Rate — lower is better → only flag high outliers (excessive discounting)
    "discount_rate":    ("DISC", "Discount Rate",    "high"),
    "gross_disc_rate":  ("DISC", "Discount Rate",    "high"),
    # Mobile Penetration — higher is better → only flag low outliers
    "mobile_pct":         ("MOBPCT", "Mobile Penetration %", "low"),
    "mobile_penetration": ("MOBPCT", "Mobile Penetration %", "low"),
    # Bill Integrity — higher is better → only flag low outliers (leakage / fraud)
    "bill_integrity": ("BILLINT", "Bill Integrity %", "low"),
    # Gross Margin — higher is better → only flag low outliers
    "gross_margin_pct": ("GM", "Gross Margin %", "low"),
}

# For UPT: derive from QTY + bill count columns if upt not present
UPT_QTY_COLS   = ["qty", "total_qty", "units_sold", "sale_qty"]
UPT_BILL_COLS  = ["bill_count", "bills_count", "transaction_count", "billno_count", "bills"]


# ─── Thresholds for rule-based detections ─────────────────────────────────────

# Discount rate at which non-promo discount is suspicious
UNAUTHORIZED_DISCOUNT_THRESHOLD = 0.05   # > 5% non-promo discount on a bill/store

# Bill integrity: NETAMT should be ≥ X% of (GROSSAMT - DISCOUNTAMT - PROMOAMT)
# Below this = unexplained leakage (possible pilferage / system manipulation)
BILL_INTEGRITY_THRESHOLD = 0.90          # < 90% integrity = flag

# High discount rate threshold (DISCOUNTAMT / GROSSAMT)
HIGH_DISCOUNT_RATE = 0.35               # > 35% discount is anomalous

# Sales return rate (return qty / total qty per store/category)
HIGH_RETURN_RATE = 0.05                  # > 5% return rate = flag

# Columns needed for each detection type
PILFERAGE_COLS  = {"netamt", "grossamt"}
DISCOUNT_COLS   = {"grossamt", "discountamt"}
PROMO_COLS      = {"discountamt", "promoamt"}
RETURN_COLS     = {"netamt"}             # negative NETAMT = return transaction


def detect_anomalies(df: pd.DataFrame) -> dict:
    """
    Run full anomaly detection on a query result DataFrame:
    - Z-score outliers on KPI columns
    - Pilferage signals (bill integrity cross-check)
    - Discount anomalies (authorized promo vs unauthorized markdown)
    - Sales return patterns (negative value/qty transactions)

    Returns anomaly summary dict with all flagged records.
    """
    if df is None or df.empty:
        return {"anomalies": [], "upt_computed": False, "pilferage": [],
                "discount_anomalies": [], "returns": []}

    df = df.copy()
    df.columns = [c.lower().strip() for c in df.columns]

    # Derive UPT if not present
    upt_computed = _derive_upt(df)

    anomalies = []

    # ── 1. Z-score statistical anomalies ──────────────────────────────────
    for col, (kpi_tag, kpi_label, bad_direction) in KPI_ANOMALY_COLUMNS.items():
        if col not in df.columns:
            continue

        series = pd.to_numeric(df[col], errors="coerce").dropna()
        if len(series) < 3:
            continue

        mean = series.mean()
        std  = series.std()
        if std == 0:
            continue

        z_scores = (series - mean) / std

        # Only flag the direction that signals a PROBLEM:
        #   "low"  → underperformers only (z ≤ -threshold)
        #   "high" → excess/overstock only  (z ≥ +threshold)
        #   "both" → either extreme
        if bad_direction == "low":
            anomaly_mask = z_scores <= -Z_THRESHOLD
        elif bad_direction == "high":
            anomaly_mask = z_scores >= Z_THRESHOLD
        else:
            anomaly_mask = z_scores.abs() >= Z_THRESHOLD

        anomaly_idx = series[anomaly_mask].index

        for idx in anomaly_idx:
            val   = df.loc[idx, col]
            z     = z_scores.loc[idx]
            direction = "spike" if z > 0 else "drop"
            severity  = "P1" if abs(z) >= 3 else "P2"
            dimension = _get_dimension(df, idx)

            anomalies.append({
                "kpi": kpi_tag,
                "kpi_label": kpi_label,
                "type": "statistical_outlier",
                "dimension": dimension,
                "value": round(float(val), 2),
                "mean": round(float(mean), 2),
                "z_score": round(float(z), 2),
                "direction": direction,
                "severity": severity,
                "description": (
                    f"{kpi_label} {direction} at {dimension}: "
                    f"{round(float(val), 2)} vs avg {round(float(mean), 2)} "
                    f"(z={round(float(z), 2)})"
                ),
            })

    # ── 2. Pilferage / Bill Integrity ─────────────────────────────────────
    pilferage_flags = _detect_pilferage(df)
    for p in pilferage_flags:
        p["type"] = "pilferage"
        p["severity"] = "P1"
        anomalies.append(p)

    # ── 3. Discount Anomalies ─────────────────────────────────────────────
    discount_flags = _detect_discount_anomalies(df)
    for d in discount_flags:
        d["type"] = "discount_anomaly"
        anomalies.append(d)

    # ── 4. Sales Returns ──────────────────────────────────────────────────
    return_flags = _detect_sales_returns(df)
    for r in return_flags:
        r["type"] = "sales_return"
        anomalies.append(r)

    # Sort: P1 first, then by abs z-score descending
    anomalies.sort(key=lambda x: (0 if x.get("severity") == "P1" else 1,
                                   -abs(x.get("z_score", 0))))

    return {
        "anomalies":          anomalies,
        "total_anomalies":    len(anomalies),
        "p1_anomalies":       sum(1 for a in anomalies if a.get("severity") == "P1"),
        "p2_anomalies":       sum(1 for a in anomalies if a.get("severity") == "P2"),
        "pilferage_count":    len(pilferage_flags),
        "discount_anom_count":len(discount_flags),
        "return_count":       len(return_flags),
        "upt_computed":       upt_computed,
    }


def _derive_upt(df: pd.DataFrame) -> bool:
    """Compute UPT column from qty and bill count if not already present."""
    if "upt" in df.columns or "units_per_transaction" in df.columns:
        return False  # Already present

    qty_col  = next((c for c in UPT_QTY_COLS  if c in df.columns), None)
    bill_col = next((c for c in UPT_BILL_COLS if c in df.columns), None)

    if qty_col and bill_col:
        qty   = pd.to_numeric(df[qty_col],  errors="coerce")
        bills = pd.to_numeric(df[bill_col], errors="coerce").replace(0, np.nan)
        df["upt"] = (qty / bills).round(2)
        logger.debug(f"UPT derived from {qty_col}/{bill_col}")
        return True

    return False


def _detect_pilferage(df: pd.DataFrame) -> list:
    """
    Pilferage Signal Detection — cross-validates bill financial fields.

    Method: Bill Integrity = NETAMT / (GROSSAMT - DISCOUNTAMT - PROMOAMT)
    If integrity < BILL_INTEGRITY_THRESHOLD → unexplained financial leakage.

    Cross-validation: also flags rows where NETAMT << GROSSAMT without
    corresponding DISCOUNTAMT + PROMOAMT accounting for the difference.
    Only fires if GROSSAMT, NETAMT both present.
    """
    flags = []
    cols = {c.lower() for c in df.columns}

    if not ({"netamt", "grossamt"} <= cols):
        return flags

    netamt   = pd.to_numeric(df["netamt"],   errors="coerce")
    grossamt = pd.to_numeric(df["grossamt"], errors="coerce")

    discountamt = pd.to_numeric(df.get("discountamt", pd.Series([0]*len(df), index=df.index)), errors="coerce").fillna(0)
    promoamt    = pd.to_numeric(df.get("promoamt",    pd.Series([0]*len(df), index=df.index)), errors="coerce").fillna(0)

    # PROMOAMT is a component of DISCOUNTAMT (not separate additive field).
    # Formula: NETAMT = GROSSAMT - DISCOUNTAMT (PROMOAMT already included in DISCOUNTAMT).
    # Non-promo discount = DISCOUNTAMT - PROMOAMT (used in discount anomaly detection).
    expected_net = grossamt - discountamt
    expected_net = expected_net.replace(0, np.nan)

    integrity = netamt / expected_net

    # Flag rows where integrity is below threshold (unexplained leakage)
    low_integrity = (
        integrity.notna() &
        (integrity < BILL_INTEGRITY_THRESHOLD) &
        (grossamt > 0) &
        (netamt >= 0)   # Exclude returns (negative = separate detection)
    )

    for idx in df[low_integrity].index:
        dim   = _get_dimension(df, idx)
        net_v = round(float(netamt.loc[idx]), 2)
        exp_v = round(float(expected_net.loc[idx]), 2)
        integ = round(float(integrity.loc[idx]), 3)
        disc  = round(float(discountamt.loc[idx]), 2)
        prom  = round(float(promoamt.loc[idx]), 2)

        flags.append({
            "kpi":       "PILFERAGE",
            "kpi_label": "Pilferage / Bill Integrity Breach",
            "dimension": dim,
            "value":     integ,
            "integrity_score": integ,
            "netamt":    net_v,
            "expected_netamt": exp_v,
            "discountamt": disc,
            "promoamt":  prom,
            "leakage_amt": round(exp_v - net_v, 2),
            "description": (
                f"Bill integrity breach at {dim}: "
                f"NETAMT={net_v} but expected {exp_v} "
                f"(GROSSAMT−DISC−PROMO). Integrity={integ:.1%}. "
                f"Unexplained leakage=₹{round(exp_v - net_v, 2)}. "
                f"Disc=₹{disc}, Promo=₹{prom}. Possible pilferage or fraud."
            ),
        })

    return flags


def _detect_discount_anomalies(df: pd.DataFrame) -> list:
    """
    Discount Anomaly Detection — identifies unauthorized / non-promo discounts.

    Three checks (all require column presence):
    1. Non-promo discount: DISCOUNTAMT > PROMOAMT (non-promotional markdown applied)
       Cross-check: rate = (DISCOUNTAMT - PROMOAMT) / GROSSAMT > threshold
    2. High total discount: DISCOUNTAMT / GROSSAMT > HIGH_DISCOUNT_RATE
    3. Z-score outlier on discount rate across stores/categories
    """
    flags = []
    cols = {c.lower() for c in df.columns}

    if "grossamt" not in cols or "discountamt" not in cols:
        return flags

    grossamt    = pd.to_numeric(df["grossamt"],    errors="coerce")
    discountamt = pd.to_numeric(df["discountamt"], errors="coerce")
    promoamt    = pd.to_numeric(df.get("promoamt", pd.Series([0]*len(df), index=df.index)), errors="coerce").fillna(0)

    # Discount rate
    gross_safe   = grossamt.replace(0, np.nan)
    disc_rate    = (discountamt / gross_safe).fillna(0)
    # Non-promo discount = discount beyond promo allocation
    non_promo    = (discountamt - promoamt).clip(lower=0)
    non_promo_rate = (non_promo / gross_safe).fillna(0)

    # ── Check 1: Unauthorized (non-promo) discount above threshold ────────
    unauthorized = (
        (non_promo_rate > UNAUTHORIZED_DISCOUNT_THRESHOLD) &
        (grossamt > 0)
    )
    for idx in df[unauthorized].index:
        dim   = _get_dimension(df, idx)
        rate  = round(float(non_promo_rate.loc[idx]) * 100, 2)
        disc_v = round(float(discountamt.loc[idx]), 2)
        prom_v = round(float(promoamt.loc[idx]), 2)
        gross_v = round(float(grossamt.loc[idx]), 2)

        severity = "P1" if rate > 20 else "P2"
        flags.append({
            "kpi":       "DISCOUNT",
            "kpi_label": "Unauthorized Discount",
            "dimension": dim,
            "value":     rate,
            "disc_rate_pct":    round(float(disc_rate.loc[idx]) * 100, 2),
            "non_promo_rate_pct": rate,
            "discountamt": disc_v,
            "promoamt":  prom_v,
            "grossamt":  gross_v,
            "severity":  severity,
            "description": (
                f"Unauthorized discount at {dim}: "
                f"Non-promo discount rate={rate}% "
                f"(DISC=₹{disc_v}, PROMO=₹{prom_v}, GROSS=₹{gross_v}). "
                f"Non-promotional markdown of ₹{round(float(non_promo.loc[idx]),2)} applied. "
                f"Cross-check: total disc rate={round(float(disc_rate.loc[idx])*100,2)}%."
            ),
        })

    # ── Check 2: Abnormally high total discount rate ───────────────────────
    high_disc = (
        (disc_rate > HIGH_DISCOUNT_RATE) &
        (grossamt > 0) &
        (~unauthorized)   # Don't double-flag
    )
    for idx in df[high_disc].index:
        dim  = _get_dimension(df, idx)
        rate = round(float(disc_rate.loc[idx]) * 100, 2)
        flags.append({
            "kpi":       "DISCOUNT",
            "kpi_label": "High Discount Rate",
            "dimension": dim,
            "value":     rate,
            "disc_rate_pct": rate,
            "discountamt": round(float(discountamt.loc[idx]), 2),
            "grossamt":  round(float(grossamt.loc[idx]), 2),
            "severity":  "P1" if rate > 50 else "P2",
            "description": (
                f"High discount rate at {dim}: {rate}% of gross sales discounted. "
                f"DISC=₹{round(float(discountamt.loc[idx]),2)}, GROSS=₹{round(float(grossamt.loc[idx]),2)}. "
                f"Verify authorization — exceeds {int(HIGH_DISCOUNT_RATE*100)}% threshold."
            ),
        })

    # ── Check 3: Z-score outlier on discount rate across group ────────────
    if len(disc_rate) >= 3 and disc_rate.std() > 0:
        z = (disc_rate - disc_rate.mean()) / disc_rate.std()
        for idx in df[z.abs() >= Z_THRESHOLD].index:
            # Skip if already flagged above
            if df.loc[idx].name in [f.get("_idx") for f in flags]:
                continue
            dim  = _get_dimension(df, idx)
            rate = round(float(disc_rate.loc[idx]) * 100, 2)
            zv   = round(float(z.loc[idx]), 2)
            severity = "P1" if abs(zv) >= 3 else "P2"
            if disc_rate.loc[idx] > disc_rate.mean():
                flags.append({
                    "kpi":       "DISCOUNT",
                    "kpi_label": "Discount Rate Outlier",
                    "dimension": dim,
                    "value":     rate,
                    "z_score":   zv,
                    "mean_rate_pct": round(float(disc_rate.mean()) * 100, 2),
                    "severity":  severity,
                    "description": (
                        f"Discount outlier at {dim}: {rate}% vs group avg "
                        f"{round(float(disc_rate.mean())*100,2)}% (z={zv}). "
                        f"Investigate for unauthorized markdowns."
                    ),
                })

    return flags


def _detect_sales_returns(df: pd.DataFrame) -> list:
    """
    Sales Returns Detection.

    Identifies:
    1. Negative NETAMT rows (return transactions) — return value & rate
    2. Negative QTY rows (return units)
    3. High return rate at store/category level (return qty / total qty)
    Cross-validation: compares return qty against sale qty for authenticity.
    """
    flags = []
    cols = {c.lower() for c in df.columns}

    if "netamt" not in cols and "qty" not in cols:
        return flags

    netamt = pd.to_numeric(df.get("netamt", pd.Series(dtype=float)), errors="coerce") if "netamt" in cols else None
    qty    = pd.to_numeric(df.get("qty",    pd.Series(dtype=float)), errors="coerce") if "qty"    in cols else None

    # ── Check 1: Negative NETAMT rows (return transactions) ───────────────
    if netamt is not None:
        total_sales = netamt[netamt > 0].sum()
        returns_mask = netamt < 0
        if returns_mask.any() and total_sales > 0:
            total_returns = netamt[returns_mask].sum()
            return_rate   = abs(total_returns) / total_sales if total_sales != 0 else 0

            if return_rate > HIGH_RETURN_RATE:
                for idx in df[returns_mask].index:
                    dim     = _get_dimension(df, idx)
                    ret_amt = round(float(netamt.loc[idx]), 2)
                    rate_pct = round(return_rate * 100, 2)
                    flags.append({
                        "kpi":       "RETURNS",
                        "kpi_label": "Sales Return",
                        "dimension": dim,
                        "value":     ret_amt,
                        "return_rate_pct": rate_pct,
                        "total_return_amt": round(float(total_returns), 2),
                        "total_sales_amt":  round(float(total_sales), 2),
                        "severity":  "P1" if return_rate > 0.15 else "P2",
                        "description": (
                            f"Sales return at {dim}: ₹{abs(ret_amt)} credited. "
                            f"Return rate={rate_pct}% of sales (₹{abs(round(float(total_returns),2))} "
                            f"of ₹{round(float(total_sales),2)}). "
                            f"{'High return rate — investigate product quality or fraud.' if return_rate > 0.10 else 'Monitor return trend.'}"
                        ),
                    })

    # ── Check 2: Negative QTY rows (unit returns) ─────────────────────────
    if qty is not None:
        total_qty     = qty[qty > 0].sum()
        return_qty_mask = qty < 0
        if return_qty_mask.any() and total_qty > 0:
            total_return_qty = qty[return_qty_mask].sum()
            qty_return_rate  = abs(total_return_qty) / total_qty

            if qty_return_rate > HIGH_RETURN_RATE:
                for idx in df[return_qty_mask].index:
                    dim      = _get_dimension(df, idx)
                    ret_qty  = round(float(qty.loc[idx]), 0)
                    rate_pct = round(qty_return_rate * 100, 2)
                    # Don't double-flag if already in returns flags
                    if not any(f["dimension"] == dim and f["kpi"] == "RETURNS" for f in flags):
                        flags.append({
                            "kpi":       "RETURNS",
                            "kpi_label": "Unit Return",
                            "dimension": dim,
                            "value":     ret_qty,
                            "return_rate_pct": rate_pct,
                            "total_return_qty": int(abs(total_return_qty)),
                            "total_sale_qty":   int(total_qty),
                            "severity":  "P1" if qty_return_rate > 0.15 else "P2",
                            "description": (
                                f"Unit returns at {dim}: {abs(int(ret_qty))} units returned. "
                                f"Return rate={rate_pct}% of sold units. "
                                f"Cross-check: {int(abs(total_return_qty))} returned of {int(total_qty)} sold."
                            ),
                        })

    return flags


def _get_dimension(df: pd.DataFrame, idx) -> str:
    """Extract best available dimension label for anomaly record."""
    dimension_cols = [
        "shrtname", "store_name", "store_code", "admsite_code", "store_id",
        "division", "section", "department", "category",
        "articlename", "articlecode", "icode",
        "region", "zone",
    ]
    for col in dimension_cols:
        if col in df.columns:
            val = df.loc[idx, col]
            if pd.notna(val) and str(val).strip():
                return str(val).strip()
    return f"row_{idx}"


def format_anomalies_for_prompt(anomaly_result: dict) -> str:
    """
    Format all anomaly types for LLM prompt injection.
    Detailed, store-wise, KPI-wise — grouped by severity (P1→P2) then KPI.
    Each record includes: store, actual value, chain avg, gap, z-score, action.
    """
    anomalies = anomaly_result.get("anomalies", [])
    if not anomalies:
        return "No anomalies detected."

    total   = len(anomalies)
    p1_cnt  = anomaly_result.get("p1_anomalies", 0)
    p2_cnt  = anomaly_result.get("p2_anomalies", 0)
    pil_cnt = anomaly_result.get("pilferage_count", 0)
    dsc_cnt = anomaly_result.get("discount_anom_count", 0)
    ret_cnt = anomaly_result.get("return_count", 0)

    lines = [
        f"╔══ ANOMALY DETECTION REPORT — {total} FINDINGS ══╗",
        f"  P1-CRITICAL: {p1_cnt}  |  P2-HIGH: {p2_cnt}  |  "
        f"Pilferage: {pil_cnt}  |  Discount Fraud: {dsc_cnt}  |  Returns: {ret_cnt}",
        f"{'─'*60}",
    ]

    # ── KPI metadata for formatting ────────────────────────────────────────
    KPI_META = {
        "SPSF":      {"unit": "₹",   "label": "Sales Per Sq Ft",     "target": "≥ ₹1,000",  "action": "Review staffing, product mix, floor productivity. Activate IST pull plan."},
        "SELL_THRU": {"unit": "%",   "label": "Sell-Through %",       "target": "≥ 95%",     "action": "Trigger markdown/promo for ageing stock. Check replenishment gaps."},
        "UPT":       {"unit": "",    "label": "Units Per Transaction", "target": "Higher better", "action": "Train staff on cross-sell. Review display and bundling strategy."},
        "DOI":       {"unit": " days","label": "Days of Inventory",   "target": "Minimise",   "action": "Reduce intake. Trigger inter-store transfer or markdown to clear stock."},
        "SALES":     {"unit": "₹",   "label": "Net Sales",            "target": "Chain avg+", "action": "Investigate footfall drop, store ops issues, local competition."},
        "SOH":       {"unit": " units","label": "Stock on Hand",      "target": "Balanced",   "action": "Review replenishment policy. Avoid overstock and stock-out."},
        "PILFERAGE": {"unit": "₹",   "label": "Bill Integrity Breach","target": "100% integrity", "action": "ESCALATE to Loss Prevention. Suspend staff pending audit."},
        "DISCOUNT":  {"unit": "%",   "label": "Discount Anomaly",     "target": "≤ 5% non-promo", "action": "AUDIT: Pull all discount logs. Verify authorization chain."},
        "RETURNS":   {"unit": "₹",   "label": "Sales Return",         "target": "≤ 5% return rate", "action": "Review return policy compliance. Check for fraud or defect pattern."},
        "ATV":       {"unit": "₹",   "label": "Avg Transaction Value", "target": "≥ ₹1,500",       "action": "Train staff on cross-sell/upsell. Review display strategy and bundling."},
        "DISC":      {"unit": "%",   "label": "Discount Rate",          "target": "≤ 8%",           "action": "AUDIT: Pull discount logs. Verify authorization. Flag non-promo markdowns."},
        "MOBPCT":    {"unit": "%",   "label": "Mobile Penetration %",   "target": "≥ 85%",          "action": "Train billing staff on mobile capture. Launch CRM incentive programme."},
        "BILLINT":   {"unit": "%",   "label": "Bill Integrity %",        "target": "100%",           "action": "ESCALATE to Loss Prevention. Audit transaction logs. Suspend pending review."},
        "GM":        {"unit": "%",   "label": "Gross Margin %",          "target": "≥ 50%",          "action": "Review cost structure. Reduce discount exposure. Shift mix to high-margin SKUs."},
    }

    def _fmt_value(val, kpi: str) -> str:
        meta = KPI_META.get(kpi, {})
        unit = meta.get("unit", "")
        if unit == "₹":
            return f"₹{val:,.2f}"
        elif unit == "%":
            return f"{val:.1f}%"
        elif unit == " days":
            return f"{val:.0f} days"
        elif unit == " units":
            return f"{val:,.0f} units"
        return str(val)

    def _gap_str(val, mean, kpi: str) -> str:
        """Return gap vs chain average with direction indicator."""
        if mean is None or mean == 0:
            return ""
        gap = val - mean
        pct = (gap / abs(mean)) * 100
        arrow = "▼" if gap < 0 else "▲"
        return f"{arrow}{abs(pct):.1f}% vs chain avg"

    # ── Group by severity → then KPI ──────────────────────────────────────
    sev_order = ["P1", "P2", "P3"]
    sev_labels = {
        "P1": "🔴 P1-CRITICAL — Immediate Action Required",
        "P2": "🟠 P2-HIGH — Action Within 24–48 Hours",
        "P3": "🟡 P3-MEDIUM — Monitor and Plan",
    }

    grouped_by_sev: dict[str, list] = {"P1": [], "P2": [], "P3": []}
    for a in anomalies:
        sev = a.get("severity", "P2")
        grouped_by_sev.setdefault(sev, []).append(a)

    for sev in sev_order:
        group = grouped_by_sev.get(sev, [])
        if not group:
            continue

        lines.append(f"\n{sev_labels[sev]} ({len(group)} stores/signals)")
        lines.append("─" * 56)

        # Sub-group by KPI within each severity
        kpi_grouped: dict[str, list] = {}
        for a in group:
            k = a.get("kpi", "OTHER")
            kpi_grouped.setdefault(k, []).append(a)

        for kpi, kpi_items in kpi_grouped.items():
            meta   = KPI_META.get(kpi, {"label": kpi, "target": "—", "action": "Investigate."})
            lines.append(f"\n  ▶ {meta['label']} ({kpi}) — {len(kpi_items)} signal(s) | Target: {meta['target']}")

            for a in kpi_items[:20]:   # cap at 20 per KPI per severity
                dim   = a.get("dimension", "Unknown")
                val   = a.get("value")
                mean  = a.get("mean")
                z     = a.get("z_score")
                atype = a.get("type", "statistical_outlier")

                # Build the detail line based on anomaly type
                if atype == "statistical_outlier":
                    val_str  = _fmt_value(val, kpi) if val is not None else "N/A"
                    mean_str = _fmt_value(mean, kpi) if mean is not None else "N/A"
                    gap_str  = _gap_str(val, mean, kpi) if (val is not None and mean is not None) else ""
                    z_str    = f"  z={z:+.2f}σ" if z is not None else ""
                    lines.append(
                        f"    • {dim:<30}  Actual={val_str}  ChainAvg={mean_str}"
                        f"  {gap_str}{z_str}"
                    )

                elif atype == "pilferage":
                    integ   = a.get("integrity_score", val)
                    leakage = a.get("leakage_amt", 0)
                    net_v   = a.get("netamt", "N/A")
                    exp_v   = a.get("expected_netamt", "N/A")
                    lines.append(
                        f"    • {dim:<30}  Integrity={integ:.1%}  Leakage=₹{leakage:,.2f}"
                        f"  (NETAMT=₹{net_v:,.2f} vs Expected=₹{exp_v:,.2f})"
                    )

                elif atype == "discount_anomaly":
                    disc_rate   = a.get("disc_rate_pct", val)
                    non_promo   = a.get("non_promo_rate_pct")
                    chain_avg   = a.get("mean_rate_pct")
                    disc_amt    = a.get("discountamt", "?")
                    gross_v     = a.get("grossamt", "?")
                    detail      = f"DiscRate={disc_rate:.1f}%"
                    if non_promo is not None:
                        detail += f"  NonPromo={non_promo:.1f}%"
                    if chain_avg is not None:
                        detail += f"  ChainAvg={chain_avg:.1f}%"
                    lines.append(
                        f"    • {dim:<30}  {detail}"
                        f"  (DISC=₹{disc_amt:,.2f}  GROSS=₹{gross_v:,.2f})"
                    )

                elif atype == "sales_return":
                    ret_rate  = a.get("return_rate_pct", 0)
                    ret_amt   = a.get("total_return_amt") or a.get("value", 0)
                    sales_amt = a.get("total_sales_amt")
                    qty_str   = ""
                    if a.get("total_return_qty") is not None:
                        qty_str = f"  ReturnUnits={a['total_return_qty']:,}"
                    sales_str = f"  Sales=₹{sales_amt:,.2f}" if sales_amt else ""
                    lines.append(
                        f"    • {dim:<30}  ReturnRate={ret_rate:.1f}%"
                        f"  ReturnAmt=₹{abs(ret_amt):,.2f}{sales_str}{qty_str}"
                    )

                else:
                    # Fallback — use existing description
                    lines.append(f"    • {dim}: {a.get('description', '')}")

            if len(kpi_items) > 20:
                lines.append(f"    ... {len(kpi_items)-20} more {kpi} signals")

            # Recommended action for this KPI
            lines.append(f"    → ACTION: {meta['action']}")

    # ── KPI-wise summary at footer ─────────────────────────────────────────
    lines.append(f"\n{'═'*60}")
    lines.append("KPI-WISE EXCEPTION SUMMARY:")

    kpi_summary: dict[str, dict] = {}
    for a in anomalies:
        k   = a.get("kpi", "OTHER")
        sev = a.get("severity", "P2")
        if k not in kpi_summary:
            kpi_summary[k] = {"P1": 0, "P2": 0, "P3": 0, "total": 0}
        kpi_summary[k][sev] = kpi_summary[k].get(sev, 0) + 1
        kpi_summary[k]["total"] += 1

    for k, counts in sorted(kpi_summary.items(), key=lambda x: -x[1]["total"]):
        meta  = KPI_META.get(k, {"label": k})
        p1_c  = counts.get("P1", 0)
        p2_c  = counts.get("P2", 0)
        label = meta.get("label", k)
        lines.append(
            f"  {label:<28} Total={counts['total']:>3}  "
            f"[P1={p1_c}  P2={p2_c}]"
        )

    if pil_cnt > 0:
        lines.append(f"\n  ⚠ PILFERAGE ALERT: {pil_cnt} store(s) with bill integrity breach — ESCALATE TO LOSS PREVENTION IMMEDIATELY")
    if dsc_cnt > 0:
        lines.append(f"  ⚠ DISCOUNT FRAUD: {dsc_cnt} instance(s) — PULL DISCOUNT LOGS AND AUDIT AUTHORIZATION CHAIN")
    if ret_cnt > 0:
        lines.append(f"  ⚠ RETURNS SPIKE: {ret_cnt} store(s) exceeding 5% return rate — INVESTIGATE PRODUCT QUALITY OR FRAUD")

    if anomaly_result.get("upt_computed"):
        lines.append("  [UPT auto-computed from QTY ÷ bill_count — verify column mapping]")

    lines.append("╚" + "═" * 59 + "╝")
    return "\n".join(lines)
