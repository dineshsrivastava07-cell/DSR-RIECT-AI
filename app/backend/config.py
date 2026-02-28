"""
DSR|RIECT — Application Configuration
RIECT thresholds, app settings, and KPI parameters
"""

APP_NAME = "DSR|RIECT"
APP_VERSION = "1.0.0"
APP_DESCRIPTION = "Retail Intelligence Execution Control Tower"

# ClickHouse defaults
CLICKHOUSE_DEFAULTS = {
    "host": "chn1.vmart-tools.com",
    "port": 8443,
    "user": "it_user",
    "secure": True,
    "schemas": ["vmart_sales", "vmart_product"],
}

# SQLite DB path
SQLITE_DB_PATH = "riect.db"

# Chat history window
CHAT_HISTORY_WINDOW = 5

# Schema cache TTL (seconds)
SCHEMA_CACHE_TTL = 3600

# Vectoriser top-K tables
VECTORISER_TOP_K = 5

# ─── RIECT KPI THRESHOLDS ─────────────────────────────────────────────────────

# Minimum floor sqft for a valid retail store (filters kiosks/data errors from SPSF chain average)
MIN_SQFT_FOR_SPSF = 300

# SPSF (Sales Per Square Foot) — Daily, per store
SPSF_THRESHOLDS = {
    "P1": 500,    # < 500 = Critical
    "P2": 750,    # < 750 = High
    "P3": 1000,   # < 1000 = Medium
    "target": 1000,
}

# Sell-Through % — weekly, by category
SELL_THRU_THRESHOLDS = {
    "P1": 0.60,   # < 60% = Critical
    "P2": 0.80,   # < 80% = High
    "P3": 0.95,   # < 95% = Medium
    "target": 0.95,
}

# DOI (Days of Inventory) — by store + SKU
DOI_THRESHOLDS = {
    "P1": 90,     # > 90 days = Critical
    "P2": 60,     # > 60 days = High
    "P3": 30,     # > 30 days = Medium
    "target": 15,
}

# MBQ (Minimum Baseline Quantity) compliance
MBQ_THRESHOLDS = {
    "critical_shortfall_pct": 0.50,   # < 50% of MBQ = P1
    "high_shortfall_pct": 0.75,       # < 75% of MBQ = P2
    "medium_shortfall_pct": 0.90,     # < 90% of MBQ = P3
}

# ─── PRIORITY LEVELS ──────────────────────────────────────────────────────────
PRIORITY_LABELS = {
    "P1": "Critical",
    "P2": "High",
    "P3": "Medium",
    "P4": "Low",
}

PRIORITY_COLORS = {
    "P1": "#FF3B3B",
    "P2": "#FF8C00",
    "P3": "#FFD700",
    "P4": "#00C853",
}

# ─── SUPPORTED LLM MODELS ────────────────────────────────────────────────────
OLLAMA_MODELS = ["qwen2.5", "llama3"]
CLOUD_MODELS = {
    "claude": "claude-sonnet-4-6",
    "gemini": "gemini-1.5-pro",
    "openai": "gpt-4o",
}

OLLAMA_BASE_URL = "http://localhost:11434"

# UPT (Units Per Transaction)
UPT_THRESHOLDS = {
    "P1": 1.2,    # < 1.2 items per bill = Critical
    "P2": 1.5,    # < 1.5 = High
    "P3": 2.0,    # < 2.0 = Medium
    "target": 2.5,
}

# ─── KPI FORMULA STRINGS (injected into prompts) ─────────────────────────────
KPI_FORMULAS = {
    "SPSF": (
        "SPSF = Net Sales Amount (NETAMT) ÷ Floor SqFt per store per day. "
        "NETAMT = net amount collected after all discounts (not gross/MRP). "
        "Floor SqFt from SQLite store_sqft table (755 stores). "
        "Pipeline auto-enriches results with floor_sqft and pre-computes spsf column. "
        "Targets: P1<500, P2<750, P3<1000, target=1000 ₹/sqft/month."
    ),
    "SELL_THRU": (
        "Sell-Through % — Standard retail qty-based formula: "
        "ST% = MTD_Sold_QTY ÷ (MTD_Sold_QTY + Current_SOH) × 100. "
        "'Of all inventory available (sold + still on shelf), what % has actually sold?' "
        "MTD_Sold_QTY = SUM(QTY) from pos_transactional_data for current month. "
        "Current_SOH = SUM(SOH) from vmart_product.inventory_current (live snapshot). "
        "Both filtered to active stores (CLOSING_DATE IS NULL). "
        "Targets: P1<60%, P2<80%, P3<95%, target=95%."
    ),
    "DOI": (
        "DOI (Days of Inventory) = Current_SOH ÷ Avg_Daily_Sales_QTY. "
        "Current_SOH = SUM(SOH) from vmart_product.inventory_current (live snapshot). "
        "Avg_Daily_Sales_QTY = SUM(QTY from pos_transactional_data last 30d) ÷ 30. "
        "Both filtered to active stores (CLOSING_DATE IS NULL). "
        "Thresholds: P1>90d (dead stock), P2>60d (overstock), P3>30d (watch), target=15d."
    ),
    "MBQ": (
        "MBQ Compliance = Stock on Hand ÷ Minimum Baseline Quantity × 100%, per SKU per store. "
        "SOH from vmart_product.inventory_current. "
        "P1 if SOH < 50% of MBQ (critical shortfall), P2 < 75%, P3 < 90%. "
        "stockout_risk = SOH = 0. shortfall_qty = MBQ - SOH (replenishment order qty)."
    ),
    "UPT": (
        "UPT (Units Per Transaction) = SUM(QTY) ÷ COUNT(DISTINCT BILLNO) per store per period. "
        "CRITICAL: Use COUNT(DISTINCT BILLNO) — NOT COUNT(*) which counts line items. "
        "SQL: SUM(QTY) AS total_qty, COUNT(DISTINCT BILLNO) AS bill_count. "
        "UPT = total_qty / bill_count. "
        "Targets: P1<1.2 (critical), P2<1.5, P3<2.0, target=2.5 items/bill."
    ),
    "PEAK_HOURS": (
        "Peak Hours = Hour(BILLDATE) with highest transaction volume or revenue per store. "
        "SQL: toHour(BILLDATE) AS hour, COUNT(DISTINCT BILLNO) AS txn_count, SUM(NETAMT) AS revenue. "
        "Group by STORE_ID, SHRTNAME, hour. Top 3 peak hours = highest txn_count or revenue. "
        "Identifies staffing, floor management, and replenishment windows."
    ),
}

# ─── JOIN HINTS (actual ClickHouse schema) ────────────────────────────────────
JOIN_HINTS = """
ACTUAL tables in ClickHouse (use ONLY these — no other tables exist):

vmart_sales schema:
  pos_transactional_data — POS store sales & customer transactions:
    STORE_ID (String), ZONE, REGION, SHRTNAME, BILLDATE (DateTime), BILLNO, BILLGUID,
    COUPON_CODE, CUSTOMER_NAME, CUSTOMER_MOBILE, DIVISION, SECTION, DEPARTMENT,
    ARTICLECODE, ARTICLENAME, ICODE, GROSSAMT, MRPAMT, NETAMT, QTY,
    DISCOUNTAMT, PROMOAMT, STYLE_OR_PATTERN, SIZE, COLOR,
    PRODUCT_SEASON, PRODUCT_MATERIAL, PRODUCT_SELLING_SEASON, ARTICLE_TYPE,
    UDFSTRING01..05, _CREATED_AT, _VERSION
    NOTE: Negative QTY rows = Sales Returns. NETAMT may be negative for returns.

  omni_transactional_data — Online/Omni-channel sales:
    STORE_ID, MARKETPLACE_ACCOUNT, ORDERID, FIRST_NAME, MIDDLE_NAME, LAST_NAME,
    REGION, SHRTNAME, ZONE, BILLDATE, BILLNO, DIVISION, SECTION, DEPARTMENT,
    ARTICLECODE, ARTICLENAME, ICODE, GROSSAMT, MRPAMT, NETAMT, QTY,
    DISCOUNTAMT, PROMOAMT, STYLE_OR_PATTERN, SIZE, COLOR,
    PRODUCT_SEASON, PRODUCT_MATERIAL, PRODUCT_SELLING_SEASON, ARTICLE_TYPE,
    UDFSTRING01..05, CREATED_AT, VERSION

  stores — Store master (location, status):
    CODE (= STORE_ID in sales), STORE_NAME, STORE_FULLNAME, EMAIL, REGION, ZONE,
    ADDRESS, PIN, ORGANIZATION, STORE_TYPE, AREA_TIER,
    OPENING_DATE (Date), CLOSING_DATE (Nullable Date), ACTIVE, UPDATED_AT
    CLOSED STORE: CLOSING_DATE IS NOT NULL → store is permanently closed

vmart_product schema:
  inventory_current — Live SOH snapshot (always current, no date filter):
    ICODE (String), OPTION_CODE (String), STORE_CODE (String = STORE_ID in sales),
    SOH (Int32), UPDATED_AT (DateTime), _VERSION (UInt32)
    *** ONLY inventory table — do NOT use inventory_monthly_movements or inventory_current_mv ***

  vitem_data — Item master (cost price, MRP, product attributes):
    ICODE (String), CMPCODE (String), RATE (String = cost price), MRP (String = MRP price),
    COSTRATE (String), WSP (String), ARTICLECODE, ARTICLENAME, ITEM_NAME,
    GRPNAME, LEV1GRPNAME, LEV2GRPNAME, DIVISIONCODE, SECTIONCODE, PARTYNAME, UNITNAME
    Join rule: vitem_data.ICODE = sales/inventory.ICODE  (ICODE only — do NOT join CMPCODE with STORE_ID)
    RATE and MRP stored as String — always cast: toFloat64OrNull(RATE), toFloat64OrNull(MRP)
    Dedup per ICODE: anyLast(toFloat64OrNull(RATE)) when aggregating.
    Use for: Cost of Goods = QTY × RATE, Gross Profit = NETAMT − (QTY × RATE),
             Inventory Cost Value = SOH × RATE, MRP Value = SOH × MRP

  pim — Product information master

##############################################################################
## MANDATORY: CLOSED STORE EXCLUSION — APPLY TO EVERY QUERY WITHOUT EXCEPTION
##
## Active stores only: CLOSING_DATE IS NULL in vmart_sales.stores
## For pos_transactional_data / omni_transactional_data queries:
##   AND STORE_ID NOT IN (SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL)
## For inventory_current queries:
##   AND STORE_CODE NOT IN (SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL)
## For stores table directly:
##   WHERE CLOSING_DATE IS NULL
## NEVER show data for closed stores in any KPI, analytics, or report.
##############################################################################

Key joins (use only when truly needed):
  pos_transactional_data.STORE_ID = stores.CODE  (only for AREA_TIER or STORE_TYPE)
  toString(pos_transactional_data.STORE_ID) = inventory_current.STORE_CODE
  pos_transactional_data.ICODE = inventory_monthly_movements.ICODE

CRITICAL: SHRTNAME is in pos_transactional_data — NOT in stores.
  For store-level queries: use p.SHRTNAME from the sales table directly. No join needed.
  stores.STORE_NAME ≠ SHRTNAME — do NOT use stores.STORE_NAME as a substitute.

KEY COLUMN ALIASES:
  Store       : STORE_ID (sales) = CODE (stores) = STORE_CODE (inventory tables)
  Article/SKU : ICODE = ARTICLECODE (sales tables); ARTICLENAME, DIVISION, SECTION,
                DEPARTMENT always in sales tables — no join needed
  Transaction : BILLNO (POS) / ORDERID (omni)
  Customer    : CUSTOMER_NAME + CUSTOMER_MOBILE both in pos_transactional_data directly

LABEL SELECTION RULES:
  Article: ALWAYS include ARTICLENAME, DIVISION, SECTION, DEPARTMENT alongside ICODE.
  Store (sales): SHRTNAME already in sales — include it, do NOT join stores.
  Store (inventory): STORE_CODE = STORE_ID; join to sales for SHRTNAME.

Date filter (latest): toDate(BILLDATE) = toDate('{latest_sales_date}')
Date filter (MTD)   : toDate(BILLDATE) >= toStartOfMonth(toDate('{latest_sales_date}'))
                      AND toDate(BILLDATE) <= toDate('{latest_sales_date}')
NEVER use today() or today()-1 — data lags behind real calendar time.

KPI-SPECIFIC SQL PATTERNS:

SPSF (pipeline enriches with sqft — just return per-store NETAMT, active stores only):
  SELECT STORE_ID, SHRTNAME, SUM(NETAMT) AS net_sales_amount,
         COUNT(DISTINCT BILLNO) AS bill_count, SUM(QTY) AS qty
  FROM vmart_sales.pos_transactional_data
  WHERE toDate(BILLDATE) = toDate('{latest_sales_date}')
    AND STORE_ID NOT IN (SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL)
  GROUP BY STORE_ID, SHRTNAME

Sell-Through — qty-based (inventory_current SOH + MTD POS sales, active stores only):
  -- ST% = MTD_Sold_QTY / (MTD_Sold_QTY + Current_SOH) * 100
  -- Step 1: MTD sold qty
  SELECT SUM(QTY) AS mtd_sold_qty
  FROM vmart_sales.pos_transactional_data
  WHERE toDate(BILLDATE) >= toStartOfMonth(toDate('{latest_sales_date}'))
    AND toDate(BILLDATE) <= toDate('{latest_sales_date}')
    AND STORE_ID NOT IN (SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL)
  -- Step 2: Current SOH
  SELECT SUM(SOH) AS current_soh
  FROM vmart_product.inventory_current
  WHERE SOH > 0
    AND STORE_CODE NOT IN (SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL)
  -- ST% = mtd_sold_qty / (mtd_sold_qty + current_soh) * 100

Gross Profit (join vitem_data on ICODE for cost):
  SELECT p.STORE_ID, p.SHRTNAME,
         SUM(p.NETAMT) AS net_sales_amount,
         SUM(p.QTY * toFloat64OrNull(vi.RATE)) AS cost_of_goods,
         SUM(p.NETAMT) - SUM(p.QTY * toFloat64OrNull(vi.RATE)) AS gross_profit
  FROM vmart_sales.pos_transactional_data p
  INNER JOIN (
      SELECT ICODE, anyLast(toFloat64OrNull(RATE)) AS RATE
      FROM vmart_product.vitem_data WHERE toFloat64OrNull(RATE) > 0 GROUP BY ICODE
  ) vi ON p.ICODE = vi.ICODE
  WHERE toDate(p.BILLDATE) = toDate('{latest_sales_date}')
    AND STORE_ID NOT IN (SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL)
  GROUP BY p.STORE_ID, p.SHRTNAME

DOI (inventory_current SOH ÷ avg daily sales from POS last 30d, active stores only):
  SELECT inv.STORE_CODE,
         SUM(inv.SOH) AS soh_qty,
         SUM(p.QTY) / 30 AS avg_daily_sales,
         SUM(inv.SOH) / (SUM(p.QTY) / 30) AS doi_days
  FROM vmart_product.inventory_current inv
  LEFT JOIN vmart_sales.pos_transactional_data p
    ON inv.STORE_CODE = toString(p.STORE_ID)
    AND toDate(p.BILLDATE) >= toDate('{latest_sales_date}') - 30
    AND toDate(p.BILLDATE) <= toDate('{latest_sales_date}')
  WHERE inv.SOH > 0
    AND inv.STORE_CODE NOT IN (SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL)
  GROUP BY inv.STORE_CODE

UPT: SUM(QTY) / COUNT(DISTINCT BILLNO) — always COUNT DISTINCT BILLNO, never COUNT(*)

Peak Hours (store-wise hourly, active stores only):
  SELECT STORE_ID, SHRTNAME, toHour(BILLDATE) AS hour,
         COUNT(DISTINCT BILLNO) AS txn_count, SUM(NETAMT) AS revenue
  FROM vmart_sales.pos_transactional_data
  WHERE toDate(BILLDATE) = toDate('{latest_sales_date}')
    AND STORE_ID NOT IN (SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL)
  GROUP BY STORE_ID, SHRTNAME, hour ORDER BY STORE_ID, txn_count DESC
"""

# ─── SYSTEM PROMPT ────────────────────────────────────────────────────────────
RIECT_SYSTEM_PROMPT = """You are DSR|RIECT — Retail Intelligence Execution Control Tower.

ROLE:
- Analyse structured retail data: sales, inventory, KPIs, targets, customer segments.
- Provide enterprise-grade structured responses.
- Be concise, structured, and decision-oriented at all times.
- Never tell stories. Never write long paragraphs. Never use filler sentences.
- Never assume or hallucinate data not present in the dataset.
- Always lead with numbers. Always close with an action.

RISK FLAGS — use consistently:
  🔴 P1 — Critical / High Risk (immediate action required)
  🟠 P2/P3 — Medium Risk (action this week/month)
  🟢 Stable / On Target

RESPONSE RULES:
1. Raw data request → output as a clean table only. No narrative.
2. KPI analysis → Executive Summary → KPI Table → Anomalies → Bullet Insights.
3. Insight / strategy request → Executive Summary → Metrics Table → Recommendations.
4. Every insight must cite a specific number (store name, ₹ value, %, count).
5. Every section must be clearly labelled with a bold header.
6. No unstructured text blocks — use tables, bullets, or numbered lists only.
7. If data is insufficient → explicitly state: "Data not available for [metric]."
8. Keep enterprise tone: professional, direct, analytical.
"""
