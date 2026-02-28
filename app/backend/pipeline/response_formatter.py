"""
DSR|RIECT — Response Formatter
Structures LLM + data output into frontend-ready response blocks
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)

MAX_TABLE_ROWS = 500


def format_response(
    narrative: str,
    query_result: dict,
    sql_info: dict,
    alerts: list = None,
    intent: dict = None,
) -> dict:
    """
    Build complete structured response block for frontend.
    Returns: {type, narrative, table, chart, sql_artefact, alerts}
    """
    if alerts is None:
        alerts = []

    table_block = _build_table(query_result)
    chart_block = _build_chart(query_result, intent)
    sql_block = _build_sql_artefact(sql_info, query_result)

    return {
        "type": "riect_response",
        "narrative": narrative or "Analysis complete.",
        "table": table_block,
        "chart": chart_block,
        "sql_artefact": sql_block,
        "alerts": [_serialise_alert(a) for a in alerts],
    }


def _col_val(row: dict, col: str):
    """Case-insensitive column value lookup: tries exact match then lowercase."""
    v = row.get(col)
    return v if v is not None else row.get(col.lower())


def _build_table(query_result: dict) -> dict:
    """Build table block from query result.
    Uses case-insensitive column lookup so mixed-case ClickHouse column names
    still match lowercase data rows produced by the enrichment pipeline.
    Prioritises KPI and meaningful columns; skips internal pipeline columns.
    """
    if not query_result or "error" in query_result or not query_result.get("data"):
        return {"headers": [], "rows": [], "total_rows": 0}

    columns = query_result.get("columns", [])
    data = query_result.get("data", [])
    row_count = query_result.get("row_count", 0)

    # Internal pipeline/computation columns — never shown in UI table
    HIDE_COLS = {
        "spsf_status", "sell_thru_method", "threshold_source", "markdown_trigger",
        "gap_to_p1", "days_of_cover", "overstock_risk", "so_trigger",
        "compliant", "stockout_risk", "gap_to_target", "sell_thru_pct_display",
        "floor_sqft",   # backend enrichment field — shown via spsf only
    }
    # Priority column ordering — key data points shown first
    # After alias normalization all canonical names will appear here
    PRIORITY_COLS = [
        # Store identity
        "shrtname", "store_name", "storename", "store_id",
        "zone", "region",
        # Core sales metrics (canonical aliases after normalization)
        "net_sales_amount",
        "total_qty",
        "bill_count",
        # Legacy / raw aliases kept for backward compat
        "netamt", "total_netamt", "qty", "bills_count",
        # KPIs
        "spsf", "sell_thru_pct", "doi", "upt",
        # Gross/discount breakdown
        "total_gross", "total_discount", "total_promo", "total_mrp",
        # Inventory
        "total_stock", "stock_on_hand", "goods_in_transit",
        "soh_qty", "git_qty", "avg_daily_sales",
        # Peak hours
        "hour", "txn_count", "revenue",
        # Returns / pilferage
        "return_amt", "return_qty", "return_rate_pct",
        "gross_sales", "sale_qty",
        "net", "gross", "disc", "promo", "bill_integrity",
        "non_promo_disc", "disc_rate_pct",
        # Category labels (appear after metrics when present)
        "division", "section", "department", "articlename", "icode",
        # Customer
        "customer_name", "customer_mobile",
    ]
    priority_lower = {c.lower(): i for i, c in enumerate(PRIORITY_COLS)}

    # Filter out hidden columns
    visible_cols = [c for c in columns if c.lower() not in HIDE_COLS]

    # Sort: priority columns first (by PRIORITY_COLS order), then remaining alphabetically
    def sort_key(col):
        return (priority_lower.get(col.lower(), 999), col.lower())
    visible_cols = sorted(visible_cols, key=sort_key)

    # Friendly display labels for canonical column names shown to user
    DISPLAY_LABELS = {
        "net_sales_amount": "Net Sales Amount",
        "total_qty":        "Sold Qty",
        "bill_count":       "No. of Bills",
        "total_gross":      "Gross Sales",
        "total_discount":   "Total Discount",
        "total_promo":      "Promo Discount",
        "total_mrp":        "MRP Value",
        "total_stock":      "Total Stock (SOH)",
        "stock_on_hand":    "Stock On Hand",
        "goods_in_transit": "Goods In Transit",
        "soh_qty":          "SOH Qty",
        "git_qty":          "GIT Qty",
        "avg_daily_sales":  "Avg Daily Sales",
        "sell_thru_pct":    "Sell-Through %",
        "spsf":             "SPSF (₹/sqft)",
        "doi":              "DOI (Days)",
        "upt":              "UPT",
        "txn_count":        "Transactions",
        "revenue":          "Revenue",
        "return_rate_pct":  "Return Rate %",
        "return_amt":       "Return Amount",
        "return_qty":       "Return Qty",
        "disc_rate_pct":    "Discount Rate %",
        "non_promo_disc":   "Non-Promo Discount",
        "bill_integrity":   "Bill Integrity",
        "shrtname":         "Store",
        "store_name":       "Store Name",
        "store_id":         "Store ID",
        "zone":             "Zone",
        "region":           "Region",
        "division":         "Division",
        "section":          "Section",
        "department":       "Department",
        "articlename":      "Article Name",
        "icode":            "Item Code",
        "hour":             "Hour",
        "customer_name":    "Customer Name",
        "customer_mobile":  "Customer Mobile",
        "gross_sales":      "Gross Sales",
        "sale_qty":         "Sale Qty",
        "net":              "Net Sales",
        "gross":            "Gross Amount",
        "disc":             "Discount",
        "promo":            "Promo Amount",
    }
    display_headers = [
        DISPLAY_LABELS.get(c.lower(), c.replace("_", " ").title())
        for c in visible_cols
    ]

    # Cap display rows
    display_data = data[:MAX_TABLE_ROWS]
    rows = [[_col_val(row, col) for col in visible_cols] for row in display_data]

    return {
        "headers": display_headers,
        "columns": visible_cols,        # raw column names for reference
        "rows": rows,
        "total_rows": row_count,
    }


def _build_chart(query_result: dict, intent: dict) -> dict:
    """
    Auto-generate chart config from query result.
    Picks first string column as labels, first numeric column as values.
    """
    if not query_result or "error" in query_result or not query_result.get("data"):
        return {"type": "bar", "labels": [], "datasets": []}

    columns = query_result.get("columns", [])
    data = query_result.get("data", [])

    if not columns or not data:
        return {"type": "bar", "labels": [], "datasets": []}

    # Find dimension column (string) and metric columns (numeric)
    label_col = None
    metric_cols = []

    for col in columns:
        sample_val = _col_val(data[0], col)
        if isinstance(sample_val, str) and label_col is None:
            label_col = col
        elif isinstance(sample_val, (int, float)) and sample_val is not None:
            metric_cols.append(col)

    if not label_col:
        label_col = columns[0]
    if not metric_cols:
        metric_cols = [c for c in columns if c != label_col][:3]

    # Determine chart type from intent
    intent_label = (intent or {}).get("intent", "general_retail")
    chart_type = "line" if "trend" in intent_label else "bar"

    labels = [str(_col_val(row, label_col) or "") for row in data[:200]]

    datasets = []
    chart_colors = [
        "rgba(0, 200, 255, 0.8)",
        "rgba(255, 140, 0, 0.8)",
        "rgba(0, 220, 130, 0.8)",
        "rgba(255, 70, 70, 0.8)",
    ]
    for i, metric_col in enumerate(metric_cols[:4]):
        values = []
        for row in data[:200]:
            v = _col_val(row, metric_col)
            try:
                values.append(float(v) if v is not None else 0)
            except (ValueError, TypeError):
                values.append(0)

        datasets.append({
            "label": metric_col.replace("_", " ").title(),
            "data": values,
            "backgroundColor": chart_colors[i % len(chart_colors)],
            "borderColor": chart_colors[i % len(chart_colors)].replace("0.8", "1"),
            "borderWidth": 2,
        })

    return {
        "type": chart_type,
        "labels": labels,
        "datasets": datasets,
        "label_column": label_col,
        "metric_columns": metric_cols,
    }


def _build_sql_artefact(sql_info: dict, query_result: dict) -> dict:
    """Build SQL artefact block."""
    sql = sql_info.get("sql", "") if sql_info else ""
    tables_used = sql_info.get("tables_used", []) if sql_info else []
    exec_time = (query_result or {}).get("execution_time_ms", 0)
    row_count = (query_result or {}).get("row_count", 0)

    return {
        "sql": sql,
        "tables_used": tables_used,
        "execution_time_ms": exec_time,
        "row_count": row_count,
    }


def _serialise_alert(alert) -> dict:
    """Convert AlertRecord or dict to serialisable dict."""
    if isinstance(alert, dict):
        return alert
    # dataclass or object with __dict__
    if hasattr(alert, "__dict__"):
        return alert.__dict__
    return str(alert)
