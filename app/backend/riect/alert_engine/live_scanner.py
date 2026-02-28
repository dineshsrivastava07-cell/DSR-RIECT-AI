"""
DSR|RIECT — Live KPI Scanner
Queries ClickHouse directly for SPSF, Sell-Through and DOI per active store.
Runs KPI engines, generates ranked P1-P4 alerts, saves to SQLite.

Called automatically at startup and on-demand via POST /api/alerts/scan.

Active-store rule: Stores with CLOSING_DATE IS NOT NULL are always excluded.
"""

import calendar
import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

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

# Maximum breach rows converted to alerts per KPI — prevents DB bloat on chain-wide issues
MAX_ALERTS_PER_KPI = 50


def run_live_scan(session_id: str = None) -> dict:
    """
    Full live KPI scan: query ClickHouse → run KPI engines → save alerts.

    Covers:
      - SPSF per store (MTD projected to full-month, vs monthly thresholds)
      - Sell-Through per store  (Qty-based: MTD_QTY / (MTD_QTY + SOH) × 100)
      - DOI per store           (SOH ÷ avg_daily_sales_qty)

    Returns:
      {alerts_generated, alerts_saved, p1, p2, p3, scan_date, days_elapsed}
      or {error, alerts_generated:0, alerts_saved:0} on failure.
    """
    try:
        from clickhouse.connector import get_client
        from settings.store_sqft_store import get_sqft_lookup_by_store_id
        from riect.kpi_engine import spsf_engine
        from riect.alert_engine.alert_generator import generate_alerts
        from riect.alert_engine.action_recommender import enrich_alerts_with_actions
        from riect.alert_engine.alert_store import save_alerts, clear_scan_alerts

        client = get_client()

        # ── Latest complete sales date (active stores, ≥10k bills) ───────────
        date_res = client.query(
            "SELECT toDate(BILLDATE) AS dt "
            "FROM vmart_sales.pos_transactional_data "
            f"WHERE {ACTIVE_STORE_FILTER} "
            "GROUP BY dt HAVING COUNT(DISTINCT BILLNO) >= 10000 "
            "ORDER BY dt DESC LIMIT 1"
        )
        if not date_res.result_rows or not date_res.result_rows[0][0]:
            return {"error": "No complete sales date found", "alerts_generated": 0, "alerts_saved": 0}

        latest_date = str(date_res.result_rows[0][0])
        latest_dt = datetime.strptime(latest_date, "%Y-%m-%d")
        days_elapsed = max(latest_dt.day, 1)
        days_in_month = calendar.monthrange(latest_dt.year, latest_dt.month)[1]

        scan_session_id = session_id or f"scan_{latest_date}"

        # Clear previous auto-scan alerts so inbox reflects current reality
        cleared = clear_scan_alerts()
        logger.info(f"Live scan: cleared {cleared} old scan alerts, scanning for {latest_date}")

        all_alerts: list[dict] = []

        # ── 1. SPSF per store — MTD projected to full month ──────────────────
        sqft_map = get_sqft_lookup_by_store_id()

        if sqft_map:
            spsf_sql = f"""
                SELECT
                    STORE_ID,
                    anyLast(SHRTNAME)   AS store_name,
                    SUM(NETAMT)         AS mtd_net_sales
                FROM vmart_sales.pos_transactional_data
                WHERE toDate(BILLDATE) >= toStartOfMonth(toDate('{latest_date}'))
                  AND toDate(BILLDATE) <= toDate('{latest_date}')
                  AND {ACTIVE_STORE_FILTER}
                GROUP BY STORE_ID
                ORDER BY mtd_net_sales ASC
            """
            spsf_res = client.query(spsf_sql)
            spsf_rows = []
            for r in spsf_res.result_rows:
                store_id = int(r[0]) if r[0] else 0
                if store_id <= 0:
                    continue
                net_sales = float(r[2]) if r[2] else 0.0
                floor_sqft = sqft_map.get(store_id, 0)
                if floor_sqft < 300 or net_sales <= 0:
                    continue
                # Project MTD SPSF → monthly (compares correctly against monthly thresholds)
                monthly_proj = round((net_sales / floor_sqft) * days_in_month / days_elapsed, 1)
                spsf_rows.append({
                    "store_id":          store_id,
                    "store_name":        str(r[1]) if r[1] else f"Store {store_id}",
                    "net_sales_amount":  net_sales,
                    "floor_sqft":        floor_sqft,
                    "spsf":              monthly_proj,  # pre-computed → engine uses directly
                })

            if spsf_rows:
                spsf_df = pd.DataFrame(spsf_rows)
                spsf_result = spsf_engine.compute_spsf(spsf_df)
                spsf_summary = spsf_engine.get_spsf_summary(spsf_result)
                spsf_breaches = spsf_engine.get_breach_rows(spsf_result).head(MAX_ALERTS_PER_KPI)
                logger.info(f"SPSF: {len(spsf_rows)} stores, {len(spsf_breaches)} breaches")
                if not spsf_breaches.empty:
                    spsf_alerts = generate_alerts(
                        {"SPSF": {"available": True, "summary": spsf_summary, "breaches": spsf_breaches}},
                        session_id=scan_session_id,
                    )
                    all_alerts.extend(spsf_alerts)
        else:
            logger.warning("Live scan: no sqft data — SPSF scan skipped")

        # ── 2. Sell-Through + DOI per store — pre-aggregated (no row multiplication) ─
        # Formula : ST%  = store_mtd_qty / (store_mtd_qty + store_soh) × 100
        # Formula : DOI  = store_soh / (store_mtd_qty / days_elapsed)
        # Source  : pre-agg inventory_current by STORE_CODE
        #           LEFT JOIN pre-agg pos_transactional_data by STORE_ID (MTD)
        # Pre-aggregating each side independently eliminates row multiplication
        # (joining on ICODE across multiple bill lines would inflate both sums)
        st_doi_sql = f"""
            SELECT
                inv.STORE_CODE                                          AS store_code,
                COALESCE(pos.store_name, concat('Store ', inv.STORE_CODE)) AS store_name,
                inv.store_soh                                           AS remaining_soh,
                COALESCE(pos.store_qty, 0)                              AS units_sold,
                CASE WHEN (COALESCE(pos.store_qty, 0) + inv.store_soh) > 0
                     THEN round(COALESCE(pos.store_qty, 0)
                                / (COALESCE(pos.store_qty, 0) + inv.store_soh) * 100, 4)
                     ELSE 0
                END                                                     AS sell_thru_pct,
                multiIf(COALESCE(pos.store_qty, 0) > 0,
                    round(inv.store_soh / (COALESCE(pos.store_qty, 0) / {days_elapsed}), 1),
                    0)                                                  AS doi_days
            FROM (
                SELECT STORE_CODE, SUM(SOH) AS store_soh
                FROM vmart_product.inventory_current
                WHERE {ACTIVE_STORE_FILTER_INV}
                GROUP BY STORE_CODE
            ) AS inv
            LEFT JOIN (
                SELECT
                    toString(STORE_ID)  AS store_code,
                    anyLast(SHRTNAME)   AS store_name,
                    SUM(QTY)            AS store_qty
                FROM vmart_sales.pos_transactional_data
                WHERE toDate(BILLDATE) >= toStartOfMonth(toDate('{latest_date}'))
                  AND toDate(BILLDATE) <= toDate('{latest_date}')
                  AND {ACTIVE_STORE_FILTER}
                GROUP BY STORE_ID
            ) AS pos ON inv.STORE_CODE = pos.store_code
            ORDER BY sell_thru_pct ASC
        """
        st_doi_res = client.query(st_doi_sql)

        # ── 3. Classify and generate Sell-Through + DOI alerts ────────────────
        # Column order: store_code, store_name, remaining_soh, units_sold, sell_thru_pct, doi_days
        from riect.alert_engine.priority_engine import classify_priority
        from riect.alert_engine.alert_generator import AlertRecord

        st_rows, doi_rows = [], []
        for r in st_doi_res.result_rows:
            store_code = str(r[0]).strip() if r[0] else ""
            store_name = str(r[1]).strip() if r[1] else f"Store {store_code}"
            soh        = float(r[2]) if r[2] else 0.0
            units_sold = float(r[3]) if r[3] else 0.0
            st_pct     = float(r[4]) if r[4] else 0.0   # already a percentage (0–100)
            doi        = float(r[5]) if r[5] else 0.0

            if soh <= 0:
                continue

            # Sell-through — pct is already a percentage value (e.g. 2.3), convert to fraction for engine
            st_fraction = st_pct / 100.0
            st_priority = classify_priority("SELL_THRU", st_fraction)
            if st_priority in ("P1", "P2", "P3"):
                gap = round(abs(0.95 - st_fraction) * 100, 1)  # gap to 95% target
                st_rows.append({
                    "store_id": store_code, "store_name": store_name,
                    "sell_thru_pct": st_fraction, "priority": st_priority,
                    "gap_to_target": gap,
                })

            # DOI
            if doi > 0:
                doi_priority = classify_priority("DOI", doi)
                if doi_priority in ("P1", "P2", "P3"):
                    doi_rows.append({
                        "store_id": store_code, "store_name": store_name,
                        "doi": doi, "priority": doi_priority,
                        "gap_to_target": round(doi - 15, 1),
                    })

        if st_rows:
            import pandas as _pd
            st_df = _pd.DataFrame(st_rows)
            st_breaches = st_df.sort_values("sell_thru_pct").head(MAX_ALERTS_PER_KPI)
            st_summary = {
                "avg_sell_thru_pct": round(st_df["sell_thru_pct"].mean() * 100, 1),
                "p1_count": int((st_df["priority"] == "P1").sum()),
                "p2_count": int((st_df["priority"] == "P2").sum()),
                "p3_count": int((st_df["priority"] == "P3").sum()),
            }
            logger.info(f"Sell-Thru: {len(st_rows)} breach stores, top {len(st_breaches)} alerting")
            from riect.kpi_engine.sell_thru_engine import get_breach_rows as _st_breach
            st_alerts = generate_alerts(
                {"SELL_THRU": {"available": True, "summary": st_summary, "breaches": st_breaches}},
                session_id=scan_session_id,
            )
            all_alerts.extend(st_alerts)

        if doi_rows:
            import pandas as _pd2
            doi_df = _pd2.DataFrame(doi_rows)
            doi_breaches = doi_df.sort_values("doi", ascending=False).head(MAX_ALERTS_PER_KPI)
            doi_summary = {
                "avg_doi": round(doi_df["doi"].mean(), 1),
                "p1_count": int((doi_df["priority"] == "P1").sum()),
                "p2_count": int((doi_df["priority"] == "P2").sum()),
                "p3_count": int((doi_df["priority"] == "P3").sum()),
            }
            logger.info(f"DOI: {len(doi_rows)} breach stores, top {len(doi_breaches)} alerting")
            doi_alerts = generate_alerts(
                {"DOI": {"available": True, "summary": doi_summary, "breaches": doi_breaches}},
                session_id=scan_session_id,
            )
            all_alerts.extend(doi_alerts)

        # ── Enrich with action playbook + save ────────────────────────────────
        all_alerts = enrich_alerts_with_actions(all_alerts)
        saved = save_alerts(all_alerts)

        p1 = sum(1 for a in all_alerts if a.get("priority") == "P1")
        p2 = sum(1 for a in all_alerts if a.get("priority") == "P2")
        p3 = sum(1 for a in all_alerts if a.get("priority") == "P3")

        logger.info(
            f"Live scan done: {len(all_alerts)} alerts "
            f"(P1={p1} P2={p2} P3={p3}), saved={saved}, date={latest_date}"
        )
        return {
            "alerts_generated": len(all_alerts),
            "alerts_saved":     saved,
            "p1":               p1,
            "p2":               p2,
            "p3":               p3,
            "scan_date":        latest_date,
            "days_elapsed":     days_elapsed,
        }

    except Exception as e:
        logger.error(f"Live scan failed: {e}", exc_info=True)
        return {"error": str(e), "alerts_generated": 0, "alerts_saved": 0}
