"""
DSR|RIECT — KPI REST API
GET /api/kpi/riect — KPI dashboard config (thresholds + alert counts)
GET /api/kpi/live  — Live chain-level KPI snapshot from ClickHouse

ACTIVE STORE RULE (applied everywhere):
  Exclude stores where CLOSING_DATE IS NOT NULL in vmart_sales.stores.
  Only ACTIVE stores (CLOSING_DATE IS NULL) count in every KPI.
"""

import calendar
import logging
from datetime import datetime

from fastapi import APIRouter
from riect.alert_engine.alert_store import get_alert_summary, get_alert_counts_by_kpi
from config import SPSF_THRESHOLDS, SELL_THRU_THRESHOLDS, DOI_THRESHOLDS, MBQ_THRESHOLDS, PRIORITY_COLORS

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/kpi", tags=["kpi"])

# Subquery to exclude closed stores — reused in all queries
ACTIVE_STORE_FILTER = (
    "STORE_ID NOT IN ("
    "SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL"
    ")"
)
ACTIVE_STORE_FILTER_INV = (
    "STORE_CODE NOT IN ("
    "SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL"
    ")"
)


@router.get("/riect")
async def get_kpi_dashboard():
    """Return KPI dashboard config — thresholds, alert counts, priority colours."""
    alert_summary = get_alert_summary()
    kpi_alerts = get_alert_counts_by_kpi()

    return {
        "alert_summary": alert_summary,
        "kpi_alerts": kpi_alerts,
        "kpi_thresholds": {
            "SPSF":     SPSF_THRESHOLDS,
            "SELL_THRU": {k: v * 100 for k, v in SELL_THRU_THRESHOLDS.items()},
            "DOI":      DOI_THRESHOLDS,
            "MBQ":      {k: v * 100 for k, v in MBQ_THRESHOLDS.items()},
        },
        "priority_colors": PRIORITY_COLORS,
        "status": "live",
    }


@router.get("/live")
async def get_kpi_live():
    """
    Chain-level KPI snapshot from ClickHouse (ACTIVE stores only).
    Returns:
      - SPSF: daily running + monthly projected (daily × days_in_month)
      - Sell-Through: MTD % (Variant A: SALE_QTY / (OPEN_QTY + IN_QTY))
                      + monthly projected %
      - DOI, UPT, ATV, discount rate
    All metrics exclude closed stores (CLOSING_DATE IS NOT NULL in stores table).
    """
    try:
        from clickhouse.connector import get_client
        from settings.store_sqft_store import get_sqft_lookup_by_store_id

        client = get_client()

        # ── Latest complete sales date (active stores only) ───────────────────
        date_res = client.query(
            "SELECT toDate(BILLDATE) AS dt "
            "FROM vmart_sales.pos_transactional_data "
            f"WHERE {ACTIVE_STORE_FILTER} "
            "GROUP BY dt HAVING COUNT(DISTINCT BILLNO) >= 10000 "
            "ORDER BY dt DESC LIMIT 1"
        )
        latest_date = str(date_res.result_rows[0][0]) if date_res.result_rows and date_res.result_rows[0][0] else None
        if not latest_date:
            return {"status": "unavailable", "error": "No complete sales date found"}

        latest_dt     = datetime.strptime(latest_date, "%Y-%m-%d")
        days_elapsed  = latest_dt.day
        days_in_month = calendar.monthrange(latest_dt.year, latest_dt.month)[1]

        # ── Chain sales aggregate for latest day (active stores only) ─────────
        sql = f"""
            SELECT
                SUM(NETAMT)              AS chain_net_sales,
                COUNT(DISTINCT STORE_ID) AS store_count,
                COUNT(DISTINCT BILLNO)   AS total_bills,
                SUM(QTY)                 AS total_qty,
                SUM(GROSSAMT)            AS chain_gross,
                SUM(DISCOUNTAMT)         AS chain_discount
            FROM vmart_sales.pos_transactional_data
            WHERE toDate(BILLDATE) = toDate('{latest_date}')
              AND {ACTIVE_STORE_FILTER}
        """
        res = client.query(sql)
        row = dict(zip(res.column_names, res.result_rows[0])) if res.result_rows else {}

        chain_net   = float(row.get("chain_net_sales") or 0)
        store_count = int(row.get("store_count") or 0)
        total_bills = int(row.get("total_bills") or 0)
        total_qty   = int(row.get("total_qty") or 0)
        chain_gross = float(row.get("chain_gross") or 0)
        chain_disc  = float(row.get("chain_discount") or 0)

        # ── SPSF: daily average across active stores ──────────────────────────
        sqft_map = get_sqft_lookup_by_store_id()
        store_spsf_sql = f"""
            SELECT STORE_ID, SUM(NETAMT) AS store_net
            FROM vmart_sales.pos_transactional_data
            WHERE toDate(BILLDATE) = toDate('{latest_date}')
              AND {ACTIVE_STORE_FILTER}
            GROUP BY STORE_ID
        """
        spsf_res = client.query(store_spsf_sql)
        spsf_values = []
        for r in spsf_res.result_rows:
            sid  = int(r[0]) if r[0] else 0
            net  = float(r[1]) if r[1] else 0
            sqft = sqft_map.get(sid, 0)
            if sqft >= 300 and net > 0:
                spsf_values.append(net / sqft)

        daily_spsf        = round(sum(spsf_values) / len(spsf_values), 1) if spsf_values else None
        monthly_spsf_proj = round(daily_spsf * days_in_month, 0) if daily_spsf else None

        # ── Sell-Through % + DOI — item-level pre-aggregation (user validated) ──
        # Formula : GROUP BY ICODE first → ST% per item → avgIf(st_pct > 0) at chain level
        # ST%     = sum(QTY) / (sum(QTY) + sum(SOH)) × 100  per ICODE
        # Chain   = average ST% across items that had sales on the day (~40-45%)
        # DOI     = sumIf(icode_soh, icode_qty > 0) / sumIf(icode_qty, icode_qty > 0)
        # Period  : single latest date (dashboard default — chatbot uses user-specified range)
        # Active stores only (CLOSING_DATE IS NOT NULL → excluded)
        sell_thru_daily_pct    = None
        sell_thru_monthly_proj = None
        doi_days               = None
        latest_inv_date        = latest_date

        st_doi_res = client.query(f"""
            SELECT
                avgIf(st_pct, st_pct > 0)                                    AS sell_thru_pct,
                multiIf(
                    sumIf(icode_qty, icode_qty > 0) > 0,
                    round(sumIf(icode_soh, icode_qty > 0)
                          / sumIf(icode_qty, icode_qty > 0), 1),
                    0
                )                                                             AS doi_days
            FROM (
                SELECT
                    i.ICODE,
                    sum(i.SOH)                                     AS icode_soh,
                    COALESCE(sum(p.QTY), 0)                        AS icode_qty,
                    multiIf(
                        (COALESCE(sum(p.QTY), 0) + sum(i.SOH)) > 0,
                        round(COALESCE(sum(p.QTY), 0)
                              / (COALESCE(sum(p.QTY), 0) + sum(i.SOH)) * 100, 2),
                        0
                    )                                              AS st_pct
                FROM vmart_product.inventory_current AS i
                LEFT JOIN vmart_sales.pos_transactional_data AS p
                    ON  i.ICODE = p.ICODE
                    AND toDate(p.BILLDATE) = toDate('{latest_date}')
                    AND p.{ACTIVE_STORE_FILTER}
                WHERE i.{ACTIVE_STORE_FILTER_INV}
                GROUP BY i.ICODE
            )
        """)

        if st_doi_res.result_rows:
            r = dict(zip(st_doi_res.column_names, st_doi_res.result_rows[0]))
            st_pct = float(r.get("sell_thru_pct") or 0)
            if st_pct > 0:
                sell_thru_daily_pct = round(st_pct, 1)
                # Monthly projection only makes sense for daily-rate metrics;
                # Sell-Through is already a ratio — cap at 100%
                sell_thru_monthly_proj = min(
                    round(st_pct / days_elapsed * days_in_month, 1), 100.0
                ) if days_elapsed < days_in_month else round(st_pct, 1)
            doi_raw = float(r.get("doi_days") or 0)
            doi_days = round(doi_raw, 1) if doi_raw > 0 else None

        # ── Derived metrics ───────────────────────────────────────────────────
        disc_rate = round(chain_disc / chain_gross * 100, 1) if chain_gross > 0 else None
        upt       = round(total_qty / total_bills, 2)        if total_bills > 0 else None
        atv       = round(chain_net / total_bills, 0)        if total_bills > 0 else None

        return {
            "status":        "live",
            "latest_date":   latest_date,
            "days_elapsed":  days_elapsed,
            "days_in_month": days_in_month,
            "chain": {
                "net_sales":              round(chain_net, 0),
                "store_count":            store_count,
                "total_bills":            total_bills,
                "total_qty":              total_qty,
                "spsf_daily":             daily_spsf,
                "spsf_monthly_projected": monthly_spsf_proj,
                "sell_thru_pct_running":  sell_thru_daily_pct,
                "sell_thru_pct_monthly":  sell_thru_monthly_proj,
                "doi_days":               doi_days,
                "latest_inv_date":        latest_inv_date,
                "disc_rate_pct":          disc_rate,
                "upt":                    upt,
                "atv":                    atv,
                # legacy key
                "spsf":                   daily_spsf,
            },
        }

    except Exception as e:
        logger.warning(f"KPI live query failed: {e}")
        return {"status": "unavailable", "error": str(e)}
