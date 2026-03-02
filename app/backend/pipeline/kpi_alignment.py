"""
DSR|RIECT — KPI Alignment Registry
Single source of truth: which columns each KPI needs to be computable.
All column aliases are lowercase (kpi_controller normalises df.columns to lowercase).
"""

# KPI_REGISTRY: {kpi_key: {"required_any": [[aliases_group1], [aliases_group2]], "category": str, "label": str}}
# A KPI is "available" if EVERY required_any group has at least one matching alias in df.columns.
# For single-group KPIs (one list), only that group must match.

KPI_REGISTRY = {
    # ── Sales & Revenue ────────────────────────────────────────────────────────
    "net_sales": {
        "required_any": [["netamt", "net_sales", "totalsales", "total_sales", "netsales", "net_sales_amount"]],
        "category": "Sales & Revenue",
        "label": "Net Sales",
    },
    # ── Billing & Basket ──────────────────────────────────────────────────────
    "atv": {
        "required_any": [
            ["netamt", "net_sales", "totalsales", "total_sales", "netsales", "net_sales_amount"],
            ["bill_count", "bills_count", "transaction_count", "billno_count", "bills", "txn_count"],
        ],
        "category": "Billing & Basket",
        "label": "ATV (Avg Transaction Value)",
    },
    "upt": {
        "required_any": [
            ["qty", "total_qty", "units_sold", "sale_qty"],
            ["bill_count", "bills_count", "transaction_count", "billno_count", "bills", "txn_count"],
        ],
        "category": "Billing & Basket",
        "label": "UPT (Units Per Transaction)",
    },
    # ── Margin & Profitability ────────────────────────────────────────────────
    "discount_rate": {
        "required_any": [
            ["discountamt", "discount_amt"],
            ["grossamt", "gross_amt"],
        ],
        "category": "Margin & Profitability",
        "label": "Gross Discount Rate",
    },
    "non_promo_disc": {
        "required_any": [
            ["discountamt", "discount_amt"],
            ["promoamt", "promo_amt"],
            ["grossamt", "gross_amt"],
        ],
        "category": "Margin & Profitability",
        "label": "Non-Promo Discount %",
    },
    "gross_margin": {
        "required_any": [
            ["netamt", "net_sales", "net_sales_amount"],
            ["cost_price", "cost_price_total", "cogs", "cost_of_goods"],
        ],
        "category": "Margin & Profitability",
        "label": "Gross Margin %",
    },
    # ── Customer ──────────────────────────────────────────────────────────────
    "unique_customers": {
        "required_any": [
            ["mobile_no", "cust_id", "customer_id", "mobile", "customer",
             "customer_mobile", "unique_customers"],
        ],
        "category": "Customer",
        "label": "Unique Customer Count",
    },
    "mobile_penetration": {
        "required_any": [
            ["mobile_no", "cust_id", "customer_id", "mobile", "customer_mobile",
             "unique_customers"],
            ["bill_count", "bills_count", "transaction_count", "billno_count", "bills", "txn_count"],
        ],
        "category": "Customer",
        "label": "Mobile Penetration %",
    },
    # ── Store Operations ──────────────────────────────────────────────────────
    "bill_integrity": {
        "required_any": [
            ["netamt", "net_sales_amount"],
            ["grossamt", "gross_amt"],
            ["discountamt", "discount_amt"],
        ],
        "category": "Store Operations",
        "label": "Bill Integrity %",
    },
    # ── Inventory Extended ────────────────────────────────────────────────────
    "soh_health": {
        "required_any": [["soh", "as_on_stk", "total_stock", "total_soh"]],
        "category": "Inventory",
        "label": "SOH Health",
    },
    "git_coverage": {
        "required_any": [["git", "in_transit", "goods_in_transit"]],
        "category": "Inventory",
        "label": "GIT Coverage",
    },
    # ── Procurement / Supply ──────────────────────────────────────────────────
    "mbq_shortfall_amt": {
        "required_any": [
            ["mbq", "min_baseline_qty"],
            ["soh", "as_on_stk", "total_soh"],
            ["cost_price", "cogs", "cost_of_goods"],
        ],
        "category": "Procurement & Supply Chain",
        "label": "MBQ Shortfall Amount",
    },
    # ── Planning & Allocation ─────────────────────────────────────────────────
    "aop_vs_actual": {
        "required_any": [
            ["aop_target", "plan_sales", "target_sales", "aop"],
            ["netamt", "net_sales", "totalsales", "net_sales_amount"],
        ],
        "category": "Planning & Allocation",
        "label": "AOP vs Actual",
    },
}


def _has_col(df_cols: set, aliases: list) -> bool:
    """True if any alias from the list is present in df_cols (all lowercase)."""
    return any(c in df_cols for c in aliases)


def detect_available_kpis(df_columns) -> dict:
    """
    Given a list/set of DataFrame column names (will be lowercased internally),
    return {kpi_key: True/False} for every KPI in the registry.

    A KPI is available when ALL required_any groups have at least one matching alias.
    """
    cols = {c.lower().strip() for c in df_columns}
    availability = {}
    for kpi_key, meta in KPI_REGISTRY.items():
        groups = meta["required_any"]
        availability[kpi_key] = all(_has_col(cols, group) for group in groups)
    return availability


def get_available_categories(availability: dict) -> list:
    """Return sorted list of categories that have at least one available KPI."""
    cats = set()
    for kpi_key, avail in availability.items():
        if avail:
            cats.add(KPI_REGISTRY[kpi_key]["category"])
    return sorted(cats)


def get_kpi_label(kpi_key: str) -> str:
    """Return human-readable label for a KPI key."""
    return KPI_REGISTRY.get(kpi_key, {}).get("label", kpi_key.upper())


def get_kpi_category(kpi_key: str) -> str:
    """Return category for a KPI key."""
    return KPI_REGISTRY.get(kpi_key, {}).get("category", "Other")
