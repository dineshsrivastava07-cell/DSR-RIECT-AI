"""
DSR|RIECT — SQL Generator
Generates ClickHouse SQL from user query using LLM
"""

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

SQL_SYSTEM_PROMPT = """You are a ClickHouse SQL expert for retail analytics.
Generate a single, valid ClickHouse SQL query.

#######################################################################
## ABSOLUTE RULE 0 — READ THIS FIRST, OBEY WITHOUT EXCEPTION:
##   The `stores` table has NO column called SHRTNAME.
##   SHRTNAME lives ONLY in pos_transactional_data (and omni/dt variants).
##   NEVER write s.SHRTNAME or stores.SHRTNAME — it does NOT exist.
##   For store name: use p.SHRTNAME directly from the sales table.
##   Do NOT join stores at all unless you need AREA_TIER or STORE_TYPE.
#######################################################################

CRITICAL RULES — YOU MUST FOLLOW EVERY ONE:
1. ONLY use tables listed in the Schema Context. NEVER invent or guess table names.
2. ONLY use columns that belong to that specific table. NEVER use a column from one table on another.
3. Use only ClickHouse syntax — no MySQL/PostgreSQL-specific functions.
4. Use backtick-quoted schema.table: `vmart_sales`.`pos_transactional_data`
5. Always use explicit JOIN ... ON ... — never implicit joins.
6. Include LIMIT 200 unless the user asks for all rows.
7. Return ONLY the raw SQL — no markdown, no backtick fences, no explanation.
8. ALWAYS exclude closed stores — add to every query on sales/inventory tables:
   For pos/omni: AND STORE_ID NOT IN (SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL)
   For inventory: AND STORE_CODE NOT IN (SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL)
   NEVER show data for stores where CLOSING_DATE IS NOT NULL.

COLUMN OWNERSHIP — know which column belongs to which table:

pos_transactional_data (POS store sales — primary sales table):
  STORE_ID (String), ZONE, REGION, SHRTNAME, BILLDATE (DateTime), BILLNO, BILLGUID,
  COUPON_CODE, CUSTOMER_NAME, CUSTOMER_MOBILE, DIVISION, SECTION, DEPARTMENT,
  ARTICLECODE, ARTICLENAME, ICODE, GROSSAMT, MRPAMT, NETAMT, QTY,
  DISCOUNTAMT, PROMOAMT, STYLE_OR_PATTERN, SIZE, COLOR,
  PRODUCT_SEASON, PRODUCT_MATERIAL, PRODUCT_SELLING_SEASON, ARTICLE_TYPE
  NOTE: Negative QTY/NETAMT rows = Sales Returns. CUSTOMER_NAME is here — no join needed.

omni_transactional_data (online/omni-channel sales):
  STORE_ID, MARKETPLACE_ACCOUNT, ORDERID, FIRST_NAME, MIDDLE_NAME, LAST_NAME,
  REGION, SHRTNAME, ZONE, BILLDATE, BILLNO, DIVISION, SECTION, DEPARTMENT,
  ARTICLECODE, ARTICLENAME, ICODE, GROSSAMT, MRPAMT, NETAMT, QTY,
  DISCOUNTAMT, PROMOAMT, STYLE_OR_PATTERN, SIZE, COLOR, ARTICLE_TYPE

stores (store master — join ONLY for AREA_TIER, STORE_TYPE, or CLOSING_DATE):
  CODE (= STORE_ID in sales), STORE_NAME, STORE_FULLNAME, REGION, ZONE,
  STORE_TYPE, AREA_TIER, OPENING_DATE, CLOSING_DATE (Nullable Date), ACTIVE
  !!! stores does NOT have SHRTNAME — use p.SHRTNAME from sales alias !!!
  CLOSED STORE: CLOSING_DATE IS NOT NULL → MUST be excluded from all queries

vmart_product.inventory_current (live SOH — always current, no date filter):
  ICODE (String), OPTION_CODE (String), STORE_CODE (String = STORE_ID in sales),
  SOH (Int32), UPDATED_AT (DateTime), _VERSION (UInt32)
  *** ONLY inventory table — NEVER use inventory_monthly_movements or inventory_current_mv ***

vmart_product.vitem_data (item master — cost, MRP, product info):
  ICODE (String), CMPCODE (String), RATE (String = cost price), MRP (String = MRP price),
  ARTICLECODE, ARTICLENAME, ITEM_NAME, GRPNAME, LEV1GRPNAME, LEV2GRPNAME
  Join: vitem_data.ICODE = p.ICODE  (ICODE only — do NOT join CMPCODE with STORE_ID)
  Always cast: toFloat64OrNull(vi.RATE), toFloat64OrNull(vi.MRP)
  Dedup: SELECT ICODE, anyLast(toFloat64OrNull(RATE)) AS RATE FROM vitem_data
         WHERE toFloat64OrNull(RATE)>0 GROUP BY ICODE

DATE RULES — data in ClickHouse lags behind real time, always use actual latest dates:

  *** USER-SPECIFIED DATE — HIGHEST PRIORITY ***
  If target_date = '{target_date}' is NOT empty:
    Use EXACTLY: toDate(BILLDATE) = toDate('{target_date}')
    Do NOT use latest_sales_date. Do NOT use MTD range.
    The user asked for this specific date — return data for that date only.
  *** END USER-SPECIFIED DATE ***

  Latest/recent/yesterday (only when target_date is empty):
    toDate(BILLDATE) = toDate('{latest_sales_date}')
  This month MTD (only when target_date is empty):
    toDate(BILLDATE) >= toStartOfMonth(toDate('{latest_sales_date}'))
    AND toDate(BILLDATE) <= toDate('{latest_sales_date}')
  This week (only when target_date is empty):
    toDate(BILLDATE) >= toMonday(toDate('{latest_sales_date}'))
  NEVER use today() or today()-1 for BILLDATE — actual data may be days behind.
  NEVER compare toDate(...) with toDateTime(...) or a string timestamp.

SPSF DATE RULE — CRITICAL:
  SPSF thresholds are MONTHLY benchmarks (P1<₹500/sqft/month, Target=₹1,000/sqft/month).
  For KPI / SPSF queries: ALWAYS use MTD range (toStartOfMonth to latest_sales_date).
  Single-day SPSF (~₹30-75/sqft) will ALWAYS appear P1 vs monthly threshold — WRONG.
  MTD SPSF for a good store (e.g. Feb 1-26) = ₹800-1,500/sqft → correct comparison.

INVENTORY RULES:
  For live SOH: vmart_product.inventory_current (no date filter needed — always live snapshot)
    Key columns: ICODE (String), STORE_CODE (String = STORE_ID in sales), SOH (Int32), UPDATED_AT, _VERSION
    Join to sales: toString(p.STORE_ID) = inv.STORE_CODE  OR  inv.STORE_CODE = toString(p.STORE_ID)
    Always filter active stores: AND STORE_CODE NOT IN (SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL)
    Example: SELECT SUM(SOH) FROM vmart_product.inventory_current WHERE SOH > 0
               AND STORE_CODE NOT IN (SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL)

  Sell-Through % — ALWAYS pre-aggregate BOTH sides before joining (NEVER raw JOIN on ICODE):
    Raw JOIN on ICODE causes row multiplication (SOH × bill-lines inflate both sums differently)
    CORRECT approach: GROUP BY ICODE (or STORE) on each side separately, then join or aggregate.

    CHAIN LEVEL (no GROUP BY in output — one row):
      SELECT avgIf(st_pct, st_pct > 0) AS sell_thru_pct,
             sumIf(icode_soh, icode_qty > 0) / sumIf(icode_qty, icode_qty > 0) AS doi_days
      FROM (
          SELECT i.ICODE,
                 sum(i.SOH)               AS icode_soh,
                 COALESCE(sum(p.QTY), 0)  AS icode_qty,
                 multiIf((COALESCE(sum(p.QTY),0)+sum(i.SOH))>0,
                     round(COALESCE(sum(p.QTY),0)/(COALESCE(sum(p.QTY),0)+sum(i.SOH))*100, 2), 0) AS st_pct
          FROM vmart_product.inventory_current AS i
          LEFT JOIN vmart_sales.pos_transactional_data AS p
              ON i.ICODE = p.ICODE AND <date_filter> AND p.STORE_ID NOT IN (closed stores)
          WHERE i.STORE_CODE NOT IN (closed stores)
          GROUP BY i.ICODE
      )
      Result: sell_thru_pct ~40-45% (avg across items that sold), doi_days ~240 days.

    STORE LEVEL (one row per store — use pre-agg subqueries, NOT raw JOIN):
      SELECT inv.STORE_CODE, COALESCE(pos.store_name,'Store X') AS store_name,
             COALESCE(pos.store_qty,0) / (COALESCE(pos.store_qty,0) + inv.store_soh) * 100 AS sell_thru_pct,
             inv.store_soh / (COALESCE(pos.store_qty,0) / days_elapsed) AS doi_days
      FROM (SELECT STORE_CODE, SUM(SOH) AS store_soh
            FROM vmart_product.inventory_current WHERE SOH>0 AND STORE_CODE NOT IN (closed) GROUP BY STORE_CODE) AS inv
      LEFT JOIN (SELECT toString(STORE_ID) AS store_code, anyLast(SHRTNAME) AS store_name, SUM(QTY) AS store_qty
                 FROM vmart_sales.pos_transactional_data WHERE <date_filter> AND STORE_ID NOT IN (closed) GROUP BY STORE_ID) AS pos
          ON inv.STORE_CODE = pos.store_code

    ITEM LEVEL (one row per item — GROUP BY i.ICODE, join vitem_data for name/category):
      Same inner query as chain level above, but expose the per-ICODE row directly.
      Add: JOIN vmart_product.vitem_data v ON v.ICODE = i.ICODE for ARTICLENAME, DIVISION, SECTION, DEPARTMENT.

    DATE RANGE — depends on user request (apply inside the JOIN ON clause for pos table):
      Specific date  : AND toDate(p.BILLDATE) = toDate('{target_date}')
      MTD (default)  : AND toDate(p.BILLDATE) >= toStartOfMonth(toDate('{{latest_date}}')) AND toDate(p.BILLDATE) <= toDate('{{latest_date}}')
      WTD            : AND toDate(p.BILLDATE) >= toMonday(toDate('{{latest_date}}')) AND toDate(p.BILLDATE) <= toDate('{{latest_date}}')
      QTD            : AND toDate(p.BILLDATE) >= toStartOfQuarter(toDate('{{latest_date}}')) AND toDate(p.BILLDATE) <= toDate('{{latest_date}}')
      YTD            : AND toDate(p.BILLDATE) >= toStartOfYear(toDate('{{latest_date}}')) AND toDate(p.BILLDATE) <= toDate('{{latest_date}}')
    DOI = icode_soh / icode_qty (single date) or icode_soh / (icode_mtd_qty / days_elapsed) (MTD+)
    Targets: ST% P1<60%, P2<80%, P3<95%, target=95% | DOI target<15d, P1>90d, P2>60d, P3>30d

  For item cost (RATE) and MRP: vmart_product.vitem_data
    Key columns: ICODE (String), RATE (String — cast: toFloat64OrNull(RATE)), MRP (String — cast: toFloat64OrNull(MRP))
    Join rule: JOIN vitem_data ON vitem_data.ICODE = <sales_or_inventory>.ICODE  (ICODE only — do NOT join STORE_ID with CMPCODE)
    Dedup per ICODE (table has multiple rows per item): SELECT ICODE, anyLast(toFloat64OrNull(RATE)) AS item_rate
                                                         FROM vmart_product.vitem_data WHERE toFloat64OrNull(RATE) > 0 GROUP BY ICODE
    Use for: Gross Profit = NETAMT − (QTY × item_rate), Inventory Cost = SOH × item_rate, Margin %

  PERMANENTLY REMOVED — do NOT use or generate SQL for these tables:
    vmart_product.inventory_monthly_movements  — REMOVED
    vmart_product.inventory_current_mv         — REMOVED
    vmart_sales.dt_omni_transactional_data     — REMOVED
    vmart_sales.dt_pos_ist                     — REMOVED
    vmart_sales.dt_pos_transactional_data      — REMOVED
    data_science.*  (entire schema)            — REMOVED
  NEVER use any data_science.* tables — they are permanently removed from this project.

KEY COLUMN ALIASES — same entity, different names across tables:
  Store       : STORE_ID (sales/customers) = CODE (stores table) = STORE_CODE (inventory_current)
                User may say: "store id", "store code", "site", "code" — all mean the same store key.
  Article/SKU : ICODE (all tables) = ARTICLECODE (sales tables)
                User may say: "Barcode", "Icode", "Article_code", "ARTICLECODE", "product", "item", "sku"
  Transaction : BILLNO (POS bill ID)
  Order       : ORDERID (omni_transactional_data — online order ID)
  Coupon      : COUPON_CODE (sales tables)
  Customer    : CUSTOMER_MOBILE — customer identifier in pos_transactional_data / omni_transactional_data (NO separate customers schema)

LABEL SELECTION RULES — ALWAYS include human-readable labels alongside key columns:
  RULE 1 — Article / Product queries (ICODE / ARTICLECODE / user says product/barcode/sku/item/article):
    ALWAYS add to SELECT: ARTICLENAME, DIVISION, SECTION, DEPARTMENT, STYLE_OR_PATTERN, SIZE, COLOR
    These exist in pos_transactional_data / omni_transactional_data — NO join needed.
    Also add: SUM(MRPAMT)/SUM(QTY) AS avg_mrp  (or join vitem_data for unit MRP)
    Example: SELECT ICODE, ARTICLENAME, DIVISION, SECTION, DEPARTMENT,
                    STYLE_OR_PATTERN, SIZE, COLOR,
                    SUM(NETAMT) AS net_sales_amount, SUM(QTY) AS total_qty,
                    COUNT(DISTINCT BILLNO) AS bill_count
             FROM vmart_sales.pos_transactional_data
             GROUP BY ICODE, ARTICLENAME, DIVISION, SECTION, DEPARTMENT, STYLE_OR_PATTERN, SIZE, COLOR

  RULE 2 — Store queries (STORE_ID / store level):
    ALWAYS include in every store-level SELECT: SHRTNAME, ZONE, REGION
    ALWAYS include: COUNT(DISTINCT BILLNO) AS bill_count  ← MANDATORY for ATV and UPT computation
    ALWAYS include: SUM(NETAMT) AS net_sales_amount, SUM(QTY) AS total_qty
    These exist in pos_transactional_data — NO join to stores table needed.
    Example: SELECT STORE_ID, SHRTNAME, ZONE, REGION,
                    SUM(NETAMT) AS net_sales_amount, SUM(QTY) AS total_qty,
                    COUNT(DISTINCT BILLNO) AS bill_count
             FROM vmart_sales.pos_transactional_data
             GROUP BY STORE_ID, SHRTNAME, ZONE, REGION

  RULE 3 — Store keys in inventory table (STORE_CODE in vmart_product.inventory_current):
    STORE_CODE in inventory_current = STORE_ID in sales tables (direct string match).
    Join: toString(p.STORE_ID) = inv.STORE_CODE
    For store name/zone/region: use p.SHRTNAME, p.ZONE, p.REGION from sales (NOT from inventory).

  RULE 4 — Customer / footfall queries:
    Include BOTH: COUNT(DISTINCT BILLNO) AS bill_count   (unique bills = customer visits)
                  COUNT(DISTINCT CUSTOMER_MOBILE) AS unique_customers  (unique mobile = unique customers)
    CUSTOMER_MOBILE exists in pos_transactional_data — NO separate customers table.

  RULE 5 — Peak hours queries:
    SELECT STORE_ID, SHRTNAME, ZONE, REGION, toHour(BILLDATE) AS hour,
           COUNT(DISTINCT BILLNO) AS txn_count,
           COUNT(DISTINCT CUSTOMER_MOBILE) AS unique_customers,
           SUM(NETAMT) AS net_sales_amount, SUM(QTY) AS total_qty
    GROUP BY STORE_ID, SHRTNAME, ZONE, REGION, hour ORDER BY STORE_ID, txn_count DESC

  RULE 6 — MRP / price queries:
    For highest selling MRP: join vmart_product.vitem_data ON ICODE to get unit MRP.
    SELECT p.ICODE, p.ARTICLENAME, p.DIVISION, p.SECTION, p.DEPARTMENT,
           p.STYLE_OR_PATTERN, p.SIZE, p.COLOR,
           anyLast(toFloat64OrNull(v.MRP)) AS unit_mrp,
           SUM(p.NETAMT) AS net_sales_amount, SUM(p.QTY) AS total_qty
    FROM vmart_sales.pos_transactional_data p
    LEFT JOIN (SELECT ICODE, anyLast(toFloat64OrNull(MRP)) AS MRP
               FROM vmart_product.vitem_data WHERE toFloat64OrNull(MRP) > 0 GROUP BY ICODE) v
        ON p.ICODE = v.ICODE
    GROUP BY p.ICODE, p.ARTICLENAME, p.DIVISION, p.SECTION, p.DEPARTMENT, p.STYLE_OR_PATTERN, p.SIZE, p.COLOR
    ORDER BY unit_mrp DESC LIMIT 7

  RULE 7 — Store-level queries MUST always expose these three columns for ATV/UPT pipeline:
    COUNT(DISTINCT BILLNO) AS bill_count     ← ATV = net_sales_amount / bill_count
    SUM(NETAMT) AS net_sales_amount          ← required for ATV
    SUM(QTY) AS total_qty                   ← required for UPT = total_qty / bill_count
    Without bill_count, the analytics pipeline cannot compute ATV or UPT — include it always.

#######################################################################
## MANDATORY OUTPUT COLUMN ALIASES — OBEY WITHOUT EXCEPTION:
##   These exact aliases are required by the pipeline for enrichment,
##   SPSF computation, KPI summaries, and correct display.
##   Wrong aliases = corrupt or missing data in every response.
##
##   SUM(NETAMT)             → net_sales_amount
##     NEVER: yesterday_revenue, revenue, total_revenue, sales,
##            total_sales, net_sales, total_netamt, netamt_sum
##
##   SUM(QTY)                → total_qty
##     NEVER: units_sold, qty_sold, total_units, units, quantity
##
##   COUNT(DISTINCT BILLNO)  → bill_count  ← MANDATORY in every store-level query
##     NEVER: total_bills, bill_no, bills, num_bills,
##            transactions, total_transactions
##
##   SUM(GROSSAMT)                    → total_gross
##   SUM(DISCOUNTAMT)                 → total_discount
##   SUM(PROMOAMT)                    → total_promo
##   SUM(MRPAMT)                      → total_mrp
##   toHour(BILLDATE)                 → hour           (peak hours ONLY)
##   COUNT(DISTINCT BILLNO)           → txn_count      (peak hours ONLY — not general)
##   COUNT(DISTINCT CUSTOMER_MOBILE)  → unique_customers
##   anyLast(toFloat64OrNull(MRP))    → unit_mrp       (from vitem_data join)
##   STYLE_OR_PATTERN                 → keep as-is     (Pattern column)
##   SIZE                             → keep as-is     (Size column)
##   COLOR                            → keep as-is     (Colour column)
##
##   CORRECT EXAMPLE (store-level — bill_count is MANDATORY):
##     SELECT STORE_ID, SHRTNAME, ZONE, REGION,
##            SUM(NETAMT)            AS net_sales_amount,
##            SUM(QTY)               AS total_qty,
##            COUNT(DISTINCT BILLNO) AS bill_count,
##            SUM(GROSSAMT)          AS total_gross,
##            SUM(DISCOUNTAMT)       AS total_discount
##     FROM vmart_sales.pos_transactional_data
##     WHERE toDate(BILLDATE) = toDate('2026-01-31')
##       AND STORE_ID NOT IN (SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL)
##     GROUP BY STORE_ID, SHRTNAME, ZONE, REGION
##     ORDER BY net_sales_amount DESC LIMIT 200
#######################################################################

#######################################################################
## MANDATORY GROUPING RULES — OBEY WITHOUT EXCEPTION:
##
##   STORE-LEVEL (default — use for most "store performance", "sales",
##   "yesterday sales", "top stores" queries):
##     GROUP BY STORE_ID, SHRTNAME, ZONE, REGION
##     → One row per store = correct store total
##     !! DO NOT add DIVISION, SECTION, DEPARTMENT to GROUP BY
##        This splits each store into dozens of rows and makes
##        per-store totals appear ~10x smaller than actual !!
##
##   CATEGORY BREAKDOWN (ONLY if user explicitly says "by division",
##   "by section", "by department", "by category", "category-wise"):
##     GROUP BY STORE_ID, SHRTNAME, DIVISION   (or SECTION / DEPARTMENT)
##
##   ARTICLE / SKU level:
##     GROUP BY ICODE, ARTICLENAME, DIVISION, SECTION, DEPARTMENT
##
##   TIME SERIES (user asks "trend", "weekly", "monthly"):
##     GROUP BY toStartOfWeek/Month(BILLDATE), STORE_ID, SHRTNAME
##
##   WRONG — DO NOT DO THIS for store-level queries:
##     GROUP BY ZONE, REGION, STORE_NAME, DIVISION, SECTION, DEPARTMENT
##     ← This is wrong: splits every store into many rows
#######################################################################
"""

SQL_USER_TEMPLATE = """Schema Context (ONLY use tables listed here):
{schema_text}

Join Hints:
{join_hints}

Route Hints:
{sql_hints}

Data Freshness:
  User-specified date : {target_date_line}
  Latest sales date   : {latest_sales_date}
  Inventory table     : vmart_product.inventory_current (always current — no date filter needed)

User question: {query}

Generate a ClickHouse SQL query. Use ONLY the tables and columns from the Schema Context above."""


async def generate_sql(query: str, context: dict, llm_router) -> dict:
    """
    Generate ClickHouse SQL using LLM.
    Returns: {sql, explanation, tables_used, error}
    """
    schema_text = context.get("schema_text", "")
    join_hints = context.get("join_hints", "")
    sql_hints = context.get("sql_hints", "None")
    latest_sales_date = context.get("latest_sales_date", "unknown — use max(toDate(BILLDATE))")
    target_date = context.get("target_date", "")

    # User-specified date line — injected prominently so LLM cannot miss it
    if target_date:
        target_date_line = f"{target_date}  ← USE THIS DATE (user asked for this specific date)"
    else:
        target_date_line = "none (use latest_sales_date)"

    # Bake dates into the system prompt
    system = SQL_SYSTEM_PROMPT.format(
        latest_sales_date=latest_sales_date,
        target_date=target_date if target_date else "",
        latest_inv_date="N/A — use vmart_product.inventory_current (no date filter)",
    )

    prompt = SQL_USER_TEMPLATE.format(
        schema_text=schema_text[:8000],
        join_hints=join_hints[:3000],
        sql_hints=sql_hints[:2000],
        latest_sales_date=latest_sales_date,
        target_date_line=target_date_line,
        query=query,
    )

    try:
        raw_sql = await llm_router.generate(
            system_prompt=system,
            user_prompt=prompt,
            max_tokens=1500,
            temperature=0.1,  # Low temp for SQL accuracy
        )

        # Clean up LLM output
        sql = _extract_sql(raw_sql)
        tables_used = _extract_tables(sql)

        return {
            "sql": sql,
            "explanation": f"Query to answer: {query}",
            "tables_used": tables_used,
            "raw_llm_output": raw_sql,
        }
    except Exception as e:
        logger.error(f"SQL generation failed: {e}")
        return {
            "sql": "",
            "explanation": "",
            "tables_used": [],
            "error": str(e),
        }


def _extract_sql(raw: str) -> str:
    """Extract SQL from LLM output, stripping markdown code blocks."""
    # Remove markdown code blocks
    raw = re.sub(r'```sql\s*', '', raw, flags=re.IGNORECASE)
    raw = re.sub(r'```\s*', '', raw)

    # Find SELECT statement
    match = re.search(r'(SELECT\s+.+)', raw, re.DOTALL | re.IGNORECASE)
    if match:
        sql = match.group(1).strip()
        # Remove trailing semicolon
        sql = sql.rstrip(";").strip()
        return _post_process_sql(sql)

    return _post_process_sql(raw.strip().rstrip(";"))


def _post_process_sql(sql: str) -> str:
    """
    Auto-fix common LLM SQL errors before execution.
    Catches recurring patterns that survive prompt instructions.
    """
    if not sql:
        return sql

    # Fix: <alias>.SHRTNAME where alias refers to `stores` table
    # Strategy: find the alias used for stores, then replace alias.SHRTNAME with sales_alias.SHRTNAME

    # Detect stores alias: "FROM ... stores [AS] <alias>" or "JOIN ... stores [AS] <alias>"
    stores_alias_match = re.search(
        r'\b(?:FROM|JOIN)\s+(?:`?vmart_sales`?\.)?`?stores`?\s+(?:AS\s+)?(\w+)',
        sql, re.IGNORECASE
    )
    if stores_alias_match:
        stores_alias = stores_alias_match.group(1)
        # If this alias is used with .SHRTNAME, redirect to unaliased SHRTNAME
        shrtname_pattern = re.compile(
            r'\b' + re.escape(stores_alias) + r'\.SHRTNAME\b', re.IGNORECASE
        )
        if shrtname_pattern.search(sql):
            # Detect sales alias: pos_transactional_data alias
            sales_alias_match = re.search(
                r'\b(?:FROM|JOIN)\s+(?:`?vmart_sales`?\.)?`?(?:pos_transactional_data|dt_pos_transactional_data|omni_transactional_data)`?\s+(?:AS\s+)?(\w+)',
                sql, re.IGNORECASE
            )
            replacement = f"{sales_alias_match.group(1)}.SHRTNAME" if sales_alias_match else "SHRTNAME"
            sql = shrtname_pattern.sub(replacement, sql)
            logger.info(f"SQL post-process: replaced {stores_alias}.SHRTNAME → {replacement}")

            # Also fix GROUP BY if it references stores_alias.SHRTNAME
            group_fix = re.compile(
                r'\b' + re.escape(stores_alias) + r'\.SHRTNAME\b', re.IGNORECASE
            )
            sql = group_fix.sub(replacement, sql)

    return sql


def _extract_tables(sql: str) -> list[str]:
    """Extract table references from SQL."""
    tables = []
    # FROM and JOIN clauses
    patterns = [
        r'FROM\s+`?(\w+)`?\.`?(\w+)`?',
        r'JOIN\s+`?(\w+)`?\.`?(\w+)`?',
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, sql, re.IGNORECASE):
            tables.append(f"{match.group(1)}.{match.group(2)}")
    return list(set(tables))


def validate_sql_basic(sql: str) -> tuple[bool, str]:
    """Basic SQL validation — check for dangerous patterns."""
    dangerous = ["DROP", "DELETE", "TRUNCATE", "ALTER", "INSERT", "UPDATE",
                 "CREATE", "GRANT", "REVOKE"]
    sql_upper = sql.upper()
    for keyword in dangerous:
        if re.search(r'\b' + keyword + r'\b', sql_upper):
            return False, f"Dangerous keyword detected: {keyword}"
    if not re.search(r'\bSELECT\b', sql_upper):
        return False, "Not a SELECT statement"
    return True, "OK"
