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
    """Format all anomaly types for LLM prompt injection — structured by category."""
    anomalies = anomaly_result.get("anomalies", [])
    if not anomalies:
        return "No anomalies detected."

    lines = [
        f"ANOMALY DETECTION REPORT — {len(anomalies)} findings "
        f"({anomaly_result.get('p1_anomalies', 0)} P1-Critical, "
        f"{anomaly_result.get('p2_anomalies', 0)} P2-High):"
    ]

    # Group by type for clarity
    type_order = ["pilferage", "discount_anomaly", "sales_return", "statistical_outlier"]
    type_labels = {
        "pilferage":         "PILFERAGE / BILL INTEGRITY",
        "discount_anomaly":  "DISCOUNT ANOMALIES",
        "sales_return":      "SALES RETURNS",
        "statistical_outlier": "STATISTICAL OUTLIERS",
    }

    grouped: dict[str, list] = {t: [] for t in type_order}
    for a in anomalies:
        t = a.get("type", "statistical_outlier")
        grouped.setdefault(t, []).append(a)

    for atype in type_order:
        group = grouped.get(atype, [])
        if not group:
            continue
        lines.append(f"\n  ▶ {type_labels.get(atype, atype)} ({len(group)} found):")
        for a in group[:8]:
            sev = a.get("severity", "P2")
            desc = a.get("description", "")
            lines.append(f"    [{sev}] {desc}")
        if len(group) > 8:
            lines.append(f"    ... {len(group)-8} more")

    # Summary counts
    pil = anomaly_result.get("pilferage_count", 0)
    dsc = anomaly_result.get("discount_anom_count", 0)
    ret = anomaly_result.get("return_count", 0)

    if pil + dsc + ret > 0:
        lines.append(
            f"\n  AUTHENTICITY FLAGS: Pilferage={pil} | "
            f"Discount Fraud={dsc} | Returns={ret}"
        )

    if anomaly_result.get("upt_computed"):
        lines.append("  [UPT auto-computed from QTY ÷ bill_count]")

    return "\n".join(lines)
