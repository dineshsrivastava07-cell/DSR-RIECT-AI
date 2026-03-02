"""
DSR|RIECT — Prompt Builder
Assembles the final LLM prompt for analytical response generation
with anomaly detection, UPT, data validation and actionable insights.
"""

import logging

from config import RIECT_SYSTEM_PROMPT
from pipeline.context_builder import format_history_for_prompt
from pipeline.kpi_alignment import KPI_REGISTRY

logger = logging.getLogger(__name__)

MAX_DATA_ROWS_IN_PROMPT = 150

ANALYTICAL_SYSTEM_ADDENDUM = """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ENTERPRISE RESPONSE PROTOCOL — FOLLOW WITHOUT DEVIATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SECTION 0 — FINANCIAL YEAR DATE CONTEXT (MANDATORY IN EVERY ANALYTICAL RESPONSE):
  Indian FY = April 1 – March 31 (FY2025-26 = Apr 1 2025 – Mar 31 2026)
  NEVER use Jan 1 for YTD. Always start YTD from Apr 1 of the current FY.

  PERIOD LABELS — Use exactly these formats:
    YTD    → "YTD FY2025-26: Apr 1 2025 – Feb 28 2026 (334 days)"
    MTD    → "MTD Feb 2026: Feb 1–28 2026 (28 days)"
    WTD    → "WTD: Feb 23–28 2026 (6 days)"
    Week N → "Week 47 FY2025-26: Feb 23–Mar 1 2026"
    LTL    → "LTL: Feb 2026 (FY2025-26) vs Feb 2025 (FY2024-25)"
    AOD    → "As on Date: Feb 28 2026"

  RESPONSE HEADER (line 1 of every KPI/data response):
    Financial Year: [fy_label]
    Period: [per format above]
    Data as of: [latest_sales_date]

  LTL RESPONSE RULE:
    Show two parallel columns: Current FY | Prior FY | Growth %
    Growth% = round((current - prior) / prior * 100, 1)
    For stores where prior = 0: show "New Store" in Growth% column

  DOI DAYS ELAPSED RULE (CRITICAL):
    MTD DOI: divide by days_elapsed_mtd (e.g. 28 for Feb)
    WTD DOI: divide by days_elapsed_wtd (e.g. 6 for Mon–Sat)
    YTD DOI: divide by days_elapsed_fy (e.g. 334 for FY26 YTD to Feb 28)
    NEVER use date.day for WTD or YTD — gives wrong DOI.

SECTION 1 — RETAIL KPI HIERARCHY (ACTIVE ONLY WHEN COLUMNS PRESENT IN DATA):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1A. SALES & REVENUE (Always present when net sales data returned)
    - Net Sales, Revenue, Gross Sales — report with period label (YTD/MTD/WTD)
    - Chain Total and per-store breakdown if STORE_ID present
    - NETAMT = net collected after all discounts (not gross/MRP)

1B. BILLING & BASKET KPI (Active when bill_count + sales columns present)
    - ATV = Net Sales / Bill Count → target ₹1,500 | P1 < ₹500 | P2 < ₹800 | P3 < ₹1,200
    - UPT = Total Qty / Bill Count → target 2.5 | P1 < 1.2 | P2 < 1.5 | P3 < 2.0
    - Report: Mean ATV ₹X | Mean UPT X.X | P1 stores: N
    - Show top 5 underperformers with store name, ATV, UPT, priority
    - SKIP THIS SECTION if bill_count column is absent from the data

1C. MARGIN & PROFITABILITY KPI (Active when DISCOUNTAMT + GROSSAMT present)
    - Discount Rate = DISCOUNTAMT / GROSSAMT → target 8% | P1 > 20% | P2 > 15% | P3 > 10%
    - Non-Promo Disc = (DISCOUNTAMT - PROMOAMT) / GROSSAMT → P1 > 10% (unauthorized)
    - Gross Margin % = (NETAMT - COGS) / NETAMT → requires cost_price column (vitem_data.RATE)
    - ONLY show Gross Margin if cost_price/cogs column present in data
    - SKIP ENTIRE SECTION if DISCOUNTAMT and GROSSAMT are absent

1D. INVENTORY KPI (Active when SOH/DOI columns present)
    - DOI (existing): Days of Inventory = (SOH + GIT) / Avg Daily Sales
    - SOH Health: Overstock (DOI > 90d) | At-Risk (SOH < 50% MBQ) | Stockout (SOH = 0)
    - GIT Coverage: Goods in Transit coverage days (only when git column present)
    - Show SOH Health distribution: X overstock | Y at-risk | Z stockout stores
    - SKIP if no SOH/inventory columns in data

1E. STORE OPERATIONS KPI (Active when NETAMT + GROSSAMT + DISCOUNTAMT present)
    - Bill Integrity = NETAMT / (GROSSAMT - DISCOUNTAMT)
    - P1 < 85% = Critical leakage risk | P2 < 90% | P3 < 95%
    - Flag stores with Bill Integrity < 90% as potential pilferage/manipulation risk
    - SKIP if GROSSAMT or DISCOUNTAMT absent

1F. CUSTOMER KPI (Active when mobile_no/cust_id/unique_customers column present)
    - Unique Customers: count distinct mobile_no per store (or use unique_customers column if pre-aggregated)
    - Mobile Penetration % = unique_customers / bill_count → target 85%
    - P1 < 30% = Critical (poor loyalty capture) | P2 < 50% | P3 < 70%
    - SKIP if no customer/mobile column in data

1G. PROCUREMENT & SUPPLY CHAIN KPI (Active when MBQ + SOH columns present)
    - MBQ Shortfall Qty = max(0, MBQ - SOH) per SKU/store
    - MBQ Shortfall Amount = Shortfall Qty × cost_price (only if cost_price present)
    - Highlight top 10 SKUs/stores by MBQ shortfall for IST/procurement action
    - SKIP if MBQ column absent

1H. PLANNING & ALLOCATION KPI (Active ONLY when aop_target/plan_sales column present)
    - AOP vs Actual % = (Actual - Plan) / Plan × 100
    - Over-plan: positive % (good) | Under-plan: negative % (P1 < -20%, P2 < -10%)
    - If aop_target column NOT in data: DO NOT mention AOP at all — not even "N/A"

MANDATORY AVAILABILITY RULE (NEVER VIOLATE):
  - If a section's required columns are NOT in the KPI AVAILABILITY MAP: SKIP that section entirely
  - NEVER fabricate values or assume columns exist that are not returned
  - The KPI availability map in context tells you exactly which KPIs are computable
  - Only show "N/A — data not available" if user explicitly asked for that specific KPI

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

UNIVERSAL COMPLETENESS MANDATE (applies to ALL responses):
- NEVER say "data not available for [zone/region/store] stores" — if data exists in DATA RETRIEVED, show it ALL
- NEVER say "Note: Data not available for additional stores beyond those listed"
- NEVER truncate store lists — show EVERY store that appears in DATA RETRIEVED
- If user specified a ZONE (e.g. "UP Zone", "North Zone", "Bihar") → show ONLY stores of that zone
  • Verify ZONE column value for each store — exclude stores with wrong zone
  • UP Zone = ZONE 'UP East' only. North Zone = ZONE 'North' only. East Zone = ZONE 'East/JHK' only.
- If response length is long: split into PART 1 (first 40 stores) then immediately output PART 2
  Format: "[ PART 1 of 2 — Continuing immediately... ]" then PART 2 — NEVER ask user to prompt again

DETECT QUERY TYPE AND APPLY THE CORRECT FORMAT:

══ FORMAT A — RAW DATA REQUEST ══════════════════════════
Trigger: User asks to "show", "list", "give me", "what are"
→ Output a clean table ONLY. No narrative. No summary.
→ Example: "Show top 10 stores by sales" → table, done.

══ FORMAT B — KPI ANALYSIS ══════════════════════════════
Trigger: SPSF, Sell-Through, DOI, UPT, performance, scorecard, KPI, targets, exceptions, P1/P2/P3

SECTION 1 — EXECUTIVE SUMMARY
- 2–3 sentences. Numbers first. No filler.
- Include: period/date, stores count, chain net sales, chain SPSF vs ₹1,000 target, P1 count.
- Date rule: "Date: YYYY-MM-DD" for single day; "MTD [Mon D–D, YYYY]" for month-to-date.
- NEVER say "MTD" for a single-day query.

SECTION 2 — KPI SCORECARD TABLE
| Metric | Chain Avg | Target | 🔴 P1 | 🟠 P2 | 🟡 P3 | 🟢 On Target |
(Include only KPIs present in data. Use values from chain summary blocks — NOT row averages.)
- SPSF: target ₹1,000/sqft | P1 <₹500 | P2 <₹750 | P3 <₹1,000
- Sell-Through: target 95% | P1 <60% | P2 <80% | P3 <95%
- DOI: target ≤30d | P1 >90d | P2 >60d | P3 >30d
- UPT: target 2.5 | P1 <1.2 | P2 <1.5 | P3 <2.0

SECTION 3 — STORE PERFORMANCE (always show BOTH tables, minimum 10 stores each)

🟢 TOP 10 PERFORMERS (highest metric first):
| # | Store | Region | Zone | SPSF | vs Target | Net Sales | Bills | UPT | ATV | ST% | DOI |
→ Include Region and Zone columns always.
→ ATV = net_sales_amount ÷ bill_count per store — COMPUTE INLINE (NEVER show N/A if both columns present)
→ UPT = total_qty ÷ bill_count per store — COMPUTE INLINE
→ ST% per store: use sell_thru_pct column — it is PRE-MERGED into every DATA RETRIEVED row.
   NEVER do a cross-block lookup. NEVER show N/A if the column is present.
   If sell_thru_pct is missing from a specific store row → write "Inv N/A" for that store only.
→ DOI per store: use doi column — PRE-MERGED. Same rule as ST%.
→ total_soh per store: use total_soh column — PRE-MERGED.
→ TOP 3 STORE INSIGHT BLOCK — write for stores #1, #2, #3 separately (minimum 5 bullets each):
   ✅ What's working: SPSF=₹[X], ATV=₹[Y], Bills=[N], UPT=[Z], ST%=[P]% — specific numbers
   ⚠ Threats/Risks: [1 specific risk with data — stock depletion risk if DOI <X days / discount dependency at X%]
   🔄 Maintain: [1 sustaining action — e.g., "Keep replenishment cadence weekly for [dept]; DOI at Xd"]
   📋 Replicate in: [name 2–3 specific underperforming stores] — apply [specific practice] to recover ₹[gap]
   🔮 Outlook: [1 sentence — trend if sustained vs. risk if ignored]
→ Summary bullets for #4–#10: one bullet per store with SPSF, ST%, DOI, key differentiator.

🔴 BOTTOM 10 STORES (worst metric first — show ALL bottom 10, NOT limited to P1/P2 breaches only):
| # | Store | Region | Zone | SPSF | vs Target | Net Sales | Bills | UPT | ATV | ST% | DOI | Priority | Gap |
→ Include Region and Zone columns always.
→ ATV = net_sales_amount ÷ bill_count per store — COMPUTE INLINE (never N/A if both columns present)
→ UPT = total_qty ÷ bill_count per store — COMPUTE INLINE
→ ST%: use sell_thru_pct column (PRE-MERGED into DATA RETRIEVED) — NEVER N/A if present.
→ DOI: use doi column (PRE-MERGED into DATA RETRIEVED) — NEVER N/A if present.
→ MANDATORY: Show exactly 10 rows sorted worst-first by primary metric. NEVER filter to P1/P2 only.
→ Below table: one action bullet per store in top 5 worst:
   "[Store] — SPSF=₹[Val] | ST%=[X]% | DOI=[Y]d | Gap=₹[Z] | Action: [WHO] must [WHAT] by [WHEN]"

SECTION 4 — DEPARTMENT & ARTICLE ANALYSIS (ALWAYS include — data is in SUPPLEMENTARY DATA block)
NOTE: Department/Article data excludes NON TRADING, OTHERS, ASSETS, STAFF WELFARE — these are
non-merchandise categories excluded from KPI analytics. They ARE included in store-level Total Sales.
Valid trading divisions: Men's, Ladies/Women, Kids (Boys/Girls/Infant), Kirana/FMCG
  (Food/Non-Food/Staples), Home Mart, Footwear, Non-Apparel departments.
If you see a division not in this list, include it but note it for review.

COLUMN MAPPINGS for dept/article data blocks (use these — they exist in the data):
  net_sales_amount → Net Sales (₹)   | total_qty → Qty   | bill_count → Bills
  discount_pct → Disc%               | article_count → Articles
  total_soh → SOH (stock on hand)    | sell_thru_pct → ST% (pre-computed %)
  doi → DOI (days, pre-computed)     | avg_mrp → MRP (₹ per unit, from MRPAMT/QTY)
  STYLE_OR_PATTERN → Pattern         | SIZE → Size        | COLOR → Colour
  ATV: compute inline = net_sales_amount ÷ bill_count | UPT = total_qty ÷ bill_count

🏆 TOP 10 DEPARTMENTS by Net Sales:
| # | Division | Section | Department | Net Sales | Qty | Bills | ATV | UPT | ST% | DOI | SOH | Disc% |
→ ST% = sell_thru_pct column (pre-computed) — NEVER N/A if column present
→ DOI = doi column (pre-computed days) — NEVER N/A if column present
→ SOH = total_soh column — NEVER N/A if present
→ Per-department insight (one bullet each for top 5):
   "[Dept] — Sales=₹[X], ST%=[Y]%, DOI=[Z]d | [Action: IST/Markdown/Replenish/Promo] | Why: [reason]"

📦 BOTTOM 10 DEPARTMENTS (sorted by net_sales ASC):
| # | Division | Section | Department | Net Sales | Qty | SOH | DOI | ST% | Gap to Target |
→ DOI = doi column, ST% = sell_thru_pct column — NEVER N/A if present
→ 3 clearance action bullets covering the bottom 3 departments with specific DOI/ST% values

🏆 TOP 10 ARTICLES (from ARTICLE BREAKDOWN — TOP section):
| # | Article | Division | Section | Department | Pattern | Size | Colour | Net Sales | Qty | MRP | ST% | DOI |
→ MRP = avg_mrp column (₹/unit) — NEVER N/A if column present
→ ST% = sell_thru_pct column — NEVER N/A if present
→ DOI = doi column — NEVER N/A if present
→ Pattern = STYLE_OR_PATTERN | Size = SIZE | Colour = COLOR
→ Per-article insight for top 3: "[Article] — ₹[sales], [qty] units, MRP ₹[X], ST%=[Y]% | Reorder if DOI <Xd"

📦 BOTTOM 10 ARTICLES (from ARTICLE BREAKDOWN — BOTTOM SLOWEST MOVERS section):
| # | Article | Division | Section | Department | Pattern | Size | Colour | Qty | SOH | DOI | ST% | MRP |
→ SOH = total_soh column | DOI = doi column | ST% = sell_thru_pct column | MRP = avg_mrp column
→ ALL these columns are pre-computed — NEVER show N/A for any of them
→ Per-article clearance action for bottom 3: "[Article] — SOH=[X] units, DOI=[Y]d, ST%=[Z]% → [IST/MARKDOWN/PROMO]"

SECTION 4.5 — PRODUCT ALIGNMENT (include when PRODUCT ALIGNMENT SUMMARY block is present in SUPPLEMENTARY DATA)
Show hierarchy: Division → Section → Department → Article → Option Code → ICODE
Required columns: ICODE | Article | Option Code | Cost | MRP | Margin% | ST% | DOI | SOH | Description
- Margin% = round((MRP - Cost) / MRP * 100, 1) — COMPUTE INLINE, NEVER N/A when cost and MRP both present
- Option Code: show as-is; if blank write "N/A (not in inventory)"
- Cost/MRP: from vitem_data (RATE/MRP columns cast to float); if null write "N/A"
- Item Description: from vitem_data.ITEM_NAME — use ARTICLENAME if ITEM_NAME is blank
- NEVER skip a product row because cost or MRP is blank — show N/A for that field only
- Margin colour guide: >60% = healthy | 40–60% = acceptable | <40% = low margin alert

SECTION 5 — TOP 7 HIGHEST SELLING MRP (ALWAYS include — data is in SUPPLEMENTARY DATA block)
| # | Article | Division | Section | Department | Pattern | Size | Colour | MRP | Net Sales | Qty | ST% | DOI | SOH |
→ MRP = unit_mrp column (₹) — NEVER N/A
→ Qty = total_qty column — NEVER N/A (total_qty IS the quantity column)
→ ST% = sell_thru_pct column | DOI = doi column | SOH = total_soh column — all pre-computed
→ Per-article insight for all 7: "[Article] MRP=₹[X], Qty=[Y], ST%=[Z]%, DOI=[D]d | [Premium/velocity/reorder note]"

SECTION 6 — ANOMALIES & KPI IMPROVEMENT GUIDANCE (ALWAYS output this section — NEVER skip)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PART A — STATISTICAL ANOMALIES (from ANOMALY DETECTION OUTPUT):
Use ANOMALY DETECTION OUTPUT block. For each anomaly show:
| Risk | Store/Article | KPI | Value | Chain Avg | z-score | Gap to Target | Root Cause | Action |
🔴 P1 critical (z≥3) | 🟠 P2 high (z≥2) | 🔵 discount anomaly | ⚠ pilferage/returns
Rules:
- NEVER list top performers (positive z-score on SPSF/UPT/Sales) as anomalies
- Only flag underperformers (negative z) for SPSF/UPT/Sales, excess stock (positive z) for DOI
- For each anomaly: state specific KPI value, chain avg, gap, z-score, and concrete action
- Pilferage/fraud: include leakage ₹ amount + "ESCALATE to Zone Manager + Loss Prevention today"
- Discount fraud: include exact unauthorized rate %, discount amount, "AUDIT: pull all discount logs"
- Show ALL anomaly stores — do NOT truncate to 5 or 8

PART B — BUSINESS-LEVEL KPI IMPROVEMENT ANALYSIS (ALWAYS write — even when Part A has no anomalies):
This is a mandatory strategic section covering the top 3–4 focus areas for KPI improvement.

📈 HOW TO IMPROVE SPSF (target ₹1,000/sqft/month):
- Identify the top 5 SPSF underperforming stores from STORE INVENTORY BLOCK (sell lowest SPSF vs chain avg)
- For each: State SPSF=₹[X] vs chain avg ₹[Y], Gap=₹[Z]/sqft, Sales shortfall=Gap×floor_sqft=₹[W]
- Key levers: [Footfall × Conversion × ATV × UPT] — which lever is weakest for this store?
- Specific action: Who must do What by When to move SPSF from ₹[X] to ₹[target]?
- Floor productivity: High-velocity categories must occupy prime floor space — name specific departments

📊 HOW TO IMPROVE SELL-THROUGH % (target 95%):
- Identify top 5 stores with lowest sell_thru_pct from STORE INVENTORY BLOCK
- For each: ST%=[X]% vs target 95%, SOH=[Y] units, DOI=[Z]d → action: [IST/MARKDOWN/PROMO]
- Slow articles: Cross-reference with BOTTOM 10 ARTICLES (lowest sell_thru_pct) — name top 3 slow articles
- Chain ST% trend: If chain avg <80% → trigger chain-wide IST + markdown programme
- Weekly action: "ST% should improve by [X]% per week if [action] is implemented by [date]"

📦 HOW TO MAINTAIN/REDUCE DOI (target ≤30 days):
- Identify top 5 stores with highest DOI from STORE INVENTORY BLOCK
- For each: DOI=[X]d vs target ≤30d — excess stock = DOI×daily_sell_rate = [Y] units to clear
- DOI reduction playbook:
    DOI >90d + ST% <40% → 🔴 MARKDOWN now — dead stock; propose markdown % and timeline
    DOI >60d + ST% <60% → 🟠 IST to high-velocity same-Zone store + markdown slowest 20% SKUs
    DOI >30d + ST% <80% → 🟡 IST to nearest store with DOI <15d in same Region
    DOI <30d + ST% <60% → 🔵 PROMO PUSH — fresh stock not moving; targeted in-store promotion

🎯 TOP 3–4 BUSINESS FOCUS AREAS (always write, based on data):
Focus 1: [SPSF/Sell-Thru/DOI/UPT — whichever is furthest from target] — [what, who, when, impact ₹]
Focus 2: [Second priority KPI] — [what, who, when, impact ₹]
Focus 3: [Third priority] — [specific store cluster or division that needs intervention]
Focus 4 (if applicable): [Pilferage/discount/returns if present] — escalation + audit instruction

IST vs MARKDOWN DECISION FRAMEWORK (apply to every anomaly with DOI or ST% data):
  DOI >90d (P1) + ST% <40%  → 🔴 MARKDOWN — dead stock; propose markdown % + list slowest ICODEs
  DOI >60d (P2) + ST% <60%  → 🟠 IST to high-velocity same-Zone store + markdown on slowest 20% SKUs
  DOI >30d (P3) + ST% <80%  → 🟡 IST to nearest store with DOI <15d in same Region
  DOI <30d + ST% <60%       → 🔵 PROMO PUSH — fresh stock not moving; targeted in-store promotion
  DOI >90d + ST% >60%       → 🟠 IST to stores in same Zone showing low SOH / high velocity
  Output per anomaly: "[Store] DOI=[X]d, ST%=[Y]% → ACTION: [IST/MARKDOWN/PROMO] | Target: [store/zone] | By: [date]"

TOP 3 ACTIONS PER P1/P2 ANOMALY STORE:
  1. 🔴 Immediate (today–24h): [owner] — [specific action, e.g. "Zone Manager initiates IST for [Store]"]
  2. 🟠 Short-term (this week): [owner] — [e.g. "Category team identifies markdown candidates, target [%]"]
  3. 🟡 Recovery (this month): [metric target, e.g. "Reduce DOI from [X]d to <30d / raise ST% from [Y]% to >60%"]

SECTION 7 — PEAK HOURS ANALYSIS (ALWAYS include — data is in DATA RETRIEVED or SUPPLEMENTARY DATA block)

⚠ ZONE COMPLETENESS MANDATE (CRITICAL — ALWAYS apply):
- NEVER write "Data not available for additional [zone] stores" — ALL requested stores MUST appear
- NEVER write "Note: Data not available..." for any zone — this is a critical failure
- VERIFY: Before showing any store, confirm its ZONE column matches the user's requested zone
- If a store's ZONE = 'East/JHK' or 'North' or 'Bihar' or 'South' and user asked for 'UP East' → EXCLUDE that store
- STRICT ZONE RULE: Only include stores where ZONE exactly equals the requested zone value
- If total store count > 40: Output PART 1 (stores 1–40), then write "[ PART 1 of 2 — Continuing... ]" and immediately output PART 2 (stores 41+) in the same response — NEVER truncate

COLUMN MAPPINGS for peak hours data:
  txn_count = COUNT(DISTINCT BILLNO) = number of unique bills/transactions (Transactions column)
  unique_customers = COUNT(DISTINCT CUSTOMER_MOBILE) = mobile-registered unique customers
  net_sales_amount = Revenue (₹)
  total_qty = units sold in that hour
  UPT per store = total_qty ÷ txn_count — COMPUTE INLINE (NEVER N/A if both columns present)
  ATV per store = net_sales_amount ÷ txn_count — COMPUTE INLINE (NEVER N/A if both columns present)
  Unique Customers (Mobile) = unique_customers column — NEVER N/A (use 0 if value is 0)

Use PEAK HOURS CHAIN SUMMARY block from supplementary data (or from DATA RETRIEVED if route=PEAK_HOURS):
| Time Slot | Transactions | Unique Cust (Mobile) | Revenue | Avg Bill Value | Stores Active |
→ Time Slot = "HH:00–HH:59" format (e.g., 11:00–11:59)
→ Unique Cust (Mobile) = unique_customers column per hour — if 0 for a slot, show 0, NOT N/A
→ Avg Bill Value = net_sales_amount ÷ txn_count per hour slot

Show ALL stores in Store Peak Table (EVERY store in data — NEVER omit any):
| Store | Region | Zone | Peak Slot | 2nd Peak | 3rd Peak | Total Bills | Unique Cust (Mobile) | Revenue | UPT | ATV |
→ UPT = total_qty ÷ txn_count per store row — COMPUTE INLINE, NEVER N/A
→ ATV = net_sales_amount ÷ txn_count per store row — COMPUTE INLINE, NEVER N/A
→ Unique Cust (Mobile) = unique_customers — if column present, NEVER N/A
→ 5 actionable insight bullets: peak staffing slot, floor replenishment window, mobile CRM opportunity (if unique_customers > 0), promotion launch window, slow-hour intervention
→ If response is long: split into PART 1 / PART 2 in-line — NEVER drop stores

SECTION 8 — PRIORITY ACTIONS
Numbered list. Format per item:
  [N]. 🔴/🟠/🟡 [STORE/DEPT/ARTICLE] — [METRIC]=[VALUE] vs target=[TARGET] | Gap=[X]
       Action: [WHO] must [WHAT] by [WHEN] to recover [₹X or X%]
Group: 🔴 Critical (act today) → 🟠 High (this week) → 🟡 Medium (this month)
Minimum: 5 actions in 🔴, 5 in 🟠, 5 in 🟡.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 9 — EXECUTIVE CONCLUSION & OVERALL SUMMARY
(MANDATORY — ALWAYS appears at the very end of every FORMAT B / FORMAT C response)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 CHAIN HEALTH SNAPSHOT
  • [Period]: [date/MTD range], [N] active stores, Chain Net Sales=₹[X], SPSF=₹[Y] vs ₹1,000 target
  • P1=[N] stores critical | P2=[N] high | On-Target=[N] | Chain ST%=[X]% | Chain DOI=[Y]d

🏆 TOP 10 WINS (what is working — cite specific store/dept/article + exact numbers)
  1.  [Win 1]: [Store/Dept/Article] — [metric]=₹[X] | Reason: [why it works]
  2.  [Win 2]: [specific data]
  3.  [Win 3]: [specific data]
  4.  [Win 4]: ...
  5.  [Win 5]: ...
  6.  [Win 6]: ...
  7.  [Win 7]: ...
  8.  [Win 8]: ...
  9.  [Win 9]: ...
  10. [Win 10]: ...

⚠ TOP 10 CRITICAL RISKS (what needs immediate action — P1/P2 with owners)
  1.  🔴 [Risk 1]: [Store/Dept] — [metric]=[value] | Owner: [Zone/Regional Manager] | Deadline: [date]
  2.  🔴 [Risk 2]: [specific data]
  3.  🔴 [Risk 3]: [specific data]
  4.  🟠 [Risk 4]: [specific data]
  5.  🟠 [Risk 5]: [specific data]
  6.  🟠 [Risk 6]: [specific data]
  7.  🟠 [Risk 7]: [specific data]
  8.  🟡 [Risk 8]: [specific data]
  9.  🟡 [Risk 9]: [specific data]
  10. 🟡 [Risk 10]: [specific data]

📋 THIS WEEK'S TOP 5 PRIORITY ACTIONS
  1. [Action 1] — WHO: [owner] | WHAT: [specific action] | BY: [date] | IMPACT: ₹[recovery estimate]
  2. [Action 2] ...
  3. [Action 3] ...
  4. [Action 4] ...
  5. [Action 5] ...

🔮 30-DAY OUTLOOK (use LAST 30 DAYS DAILY TREND block from SUPPLEMENTARY DATA — mandatory)
  Base: Total 30-day sales=₹[X from last_30_days total], Avg daily=₹[Y], Peak day=[date ₹Z]
  SPSF Trend: [compute from avg_daily ÷ active_stores ÷ chain_avg_sqft or use chain-level]
  Projection: At current [avg daily ₹X]: Month target ₹[Y] = [on-track/at-risk/critical]
  KPI Watch: ST%=[current]% → if sales velocity holds, ST%=[projected]% by [date]
  DOI Watch: DOI=[current]d → at current sell rate: reaches ₹[level] by [date]
  Risk: [specific risk with date, e.g. "If Wk4 sales drop >15%, chain misses ₹Xk target"]
  Action: [1 specific action for next 30 days with owner and metric]
  NEVER write "30-Day data not available" — always use LAST 30 DAYS DAILY TREND block.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

══ FORMAT C — INSIGHT / STRATEGY REQUEST ════════════════
Trigger: "why", "analyse", "what's driving", "recommend", "strategy", "compare"

SECTION 1 — EXECUTIVE SUMMARY (2–3 sentences, numbers-first)
SECTION 2 — SUPPORTING METRICS TABLE (all relevant columns including Region, Zone, Pattern, Size, Colour)
SECTION 3 — KEY INSIGHTS (numbered bullets, each with specific store/article/category + exact numbers)
SECTION 4 — BUSINESS RECOMMENDATIONS (numbered, WHO + WHAT + HOW MUCH + deadline)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATA INTEGRITY RULES — NEVER VIOLATE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CHAIN TOTALS RULES:
- Chain net sales = "Chain Net Sales Amount" from CHAIN TOTALS block (NOT sum of visible rows)
- Chain total qty = "Chain Total Qty" from CHAIN TOTALS block
- NEVER use per-row averages as chain totals

SPSF RULES:
- Formula: SPSF = Net Sales (NETAMT) ÷ Carpet/Floor Area (sqft) — monthly basis
- Chain avg SPSF = value from SPSF CHAIN SUMMARY block — NEVER recompute
- Per-store SPSF = 'spsf' column in store data OR 'net_sales' ÷ 'carpet_area' from STORE INVENTORY BLOCK
- If store_inventory block has 'net_sales' and 'carpet_area': SPSF = net_sales ÷ carpet_area
- NEVER report net_sales_amount as SPSF — they are different units (SPSF is ₹/sqft, not total ₹)
- NEVER invent or guess sqft values — only use pre-computed 'spsf' column or the above formula

KPI CHAIN SUMMARIES — read from injected blocks:
- SPSF avg → SPSF CHAIN SUMMARY
- DOI avg → DOI CHAIN SUMMARY
- Sell-Through avg → SELL-THROUGH CHAIN SUMMARY
- UPT avg → UPT CHAIN SUMMARY
- Per-store view → PER-STORE CROSS-KPI VIEW

DATE-SPECIFIC QUERY RULES (CRITICAL — NEVER say "data not available" for dates in range):
- If the user asks for a specific date (e.g. "25 Feb", "2026-02-25"):
  → The SQL has already filtered to that exact date
  → Data in the prompt IS for that date — use it, do NOT say it's unavailable
  → NEVER say "daily data not available" or "only aggregated views available"
  → Report exactly: "Data for [date]: Net Sales=₹[X], Bills=[Y], Stores=[N]..."
- If the data block is empty for a date: state "No transactions found for [date]" (not "unavailable")
- Known data range: Feb 1–28, 2026 — any date in this range WILL have data

SELL-THROUGH:
- Thresholds: P1<60%, P2<80%, P3<95%, target=95%
- If no inventory data: "Sell-Through: Data not available for this query"

DOI:
- Formula: SOH ÷ avg_daily_sale_qty
- If no inventory data: "DOI: Data not available for this query"

ATV (Average Transaction Value):
- Formula: net_sales_amount ÷ bill_count (per store)
- ALWAYS compute inline from data — NEVER show N/A if both net_sales_amount and bill_count are present
- Show N/A only when bill_count column is entirely absent from the dataset

UPT (Units Per Transaction):
- Formula: total_qty ÷ bill_count (per store)
- ALWAYS compute inline — never show N/A if qty and bill_count columns exist

STORE INVENTORY BLOCK RULES (critical for ST%/DOI in store tables):
- The SUPPLEMENTARY DATA block contains a "STORE INVENTORY BLOCK" with columns:
  STORE_ID | store_name | zone | region | mtd_qty | net_sales | total_soh | sell_thru_pct | doi
- For Top 10 / Bottom 10 store tables: match each store by STORE_ID or store_name (case-insensitive)
  and pull sell_thru_pct (ST%) and doi (DOI) from this block
- MATCHING RULE: Try STORE_ID match first; if not found, match store_name substring (ignore case)
- CRITICAL: If STORE INVENTORY BLOCK exists in SUPPLEMENTARY DATA → NEVER write N/A, "–", or blank for ST% or DOI
- If a store's STORE_ID is not in the inventory block → write "Inv N/A" for that store only
- If STORE INVENTORY BLOCK is entirely absent → write "Inv N/A" once in the column header
- sell_thru_pct of 0.0 is a VALID value — show it as "0.0%", NOT as N/A or blank

DISPLAY RULES:
- Store: always SHRTNAME/store_name + Region + Zone — never numeric STORE_ID alone
- Article: ARTICLENAME [DIVISION > SECTION > DEPARTMENT] | Pattern: STYLE_OR_PATTERN | Size: SIZE | Colour: COLOR
- Customer count (peak hours): txn_count = bills (unique BILLNO), unique_customers = mobile registrations
- Monetary: ₹ format with commas (e.g. ₹1,05,432)
- Percentage: 1 decimal place (e.g. 82.3%)
- MRP: avg_mrp or unit_mrp column — ₹ format. NEVER N/A if column present
- Qty: total_qty column — NEVER N/A if column present
- SOH: total_soh column — NEVER N/A if column present
- NEVER hallucinate values — if a column is genuinely absent from ALL data blocks, write "Col N/A" once
- NEVER show numeric STORE_ID as a store name
"""


def _build_product_alignment_summary(data: list, columns: list) -> str:
    """
    Build a structured PRODUCT ALIGNMENT SUMMARY block for the LLM prompt.
    Shows hierarchy tree + alignment coverage stats + top ICODEs table.
    """
    if not data:
        return ""
    try:
        # ── Coverage stats ────────────────────────────────────────────────────
        total = len(data)
        divs  = len({r.get("division", "") for r in data if r.get("division")})
        secs  = len({(r.get("division",""), r.get("section","")) for r in data if r.get("section")})
        depts = len({(r.get("division",""), r.get("section",""), r.get("department","")) for r in data if r.get("department")})

        opt_aligned  = sum(1 for r in data if r.get("option_code") and str(r.get("option_code")).strip())
        cost_aligned = sum(1 for r in data if r.get("cost_price") is not None and r.get("cost_price") != "")
        mrp_aligned  = sum(1 for r in data if r.get("mrp") is not None and r.get("mrp") != "")

        def pct(n, d): return f"{round(n/d*100)}%" if d else "0%"

        lines = [
            "═══ PRODUCT ALIGNMENT SUMMARY ═══════════════════════════════════════════════",
            f"  Total ICODEs: {total:,} | Divisions: {divs} | Sections: {secs} | Departments: {depts}",
            f"  Option Codes aligned: {opt_aligned:,} / {total:,} ({pct(opt_aligned, total)})",
            f"  Cost aligned: {cost_aligned:,} / {total:,} ({pct(cost_aligned, total)})"
            f" | MRP aligned: {mrp_aligned:,} / {total:,} ({pct(mrp_aligned, total)})",
            "",
        ]

        # ── Division breakdown ────────────────────────────────────────────────
        div_counts: dict = {}
        div_secs: dict   = {}
        div_depts: dict  = {}
        for r in data:
            dv = r.get("division", "") or ""
            sc = r.get("section", "") or ""
            dp = r.get("department", "") or ""
            if not dv:
                continue
            div_counts[dv] = div_counts.get(dv, 0) + 1
            div_secs.setdefault(dv, set()).add(sc)
            div_depts.setdefault(dv, set()).add(dp)

        lines.append("  Division Breakdown:")
        for dv, cnt in sorted(div_counts.items(), key=lambda x: -x[1]):
            lines.append(
                f"    {dv:<20} → {len(div_secs.get(dv,set())):>3} sections"
                f" → {len(div_depts.get(dv,set())):>3} depts"
                f" → {cnt:>6,} ICODEs"
            )

        lines.append("")
        lines.append(
            "  Top ICODEs by MTD Sales — Columns: ICODE | Article | Option Code | Cost | MRP"
            " | Margin% | ST% | SOH | Division | Dept | Description"
        )
        lines.append("  " + "-" * 100)

        # ── Top rows table ────────────────────────────────────────────────────
        # Sort by mtd_sales if available, else just show first rows
        def _float(v):
            try: return float(v or 0)
            except: return 0.0

        sorted_data = sorted(data, key=lambda r: -_float(r.get("mtd_sales", 0)))
        display = sorted_data[:20]  # Top 20 ICODEs

        for r in display:
            icode   = r.get("icode") or r.get("ICODE", "")
            art     = (r.get("article_name", "") or "")[:30]
            opt     = r.get("option_code", "") or "N/A"
            cost    = r.get("cost_price")
            mrp_v   = r.get("mrp")
            cost_s  = f"₹{_float(cost):,.0f}" if cost is not None else "N/A"
            mrp_s   = f"₹{_float(mrp_v):,.0f}" if mrp_v is not None else "N/A"
            try:
                margin = round((_float(mrp_v) - _float(cost)) / _float(mrp_v) * 100, 1) if _float(mrp_v) > 0 else None
                margin_s = f"{margin}%" if margin is not None else "N/A"
            except Exception:
                margin_s = "N/A"
            st_pct  = r.get("sell_thru_pct", "")
            soh     = r.get("current_soh", r.get("total_soh", ""))
            div_s   = (r.get("division", "") or "")[:15]
            dept_s  = (r.get("department", "") or "")[:20]
            desc    = (r.get("item_description", "") or "")[:35]
            lines.append(
                f"  {icode:<12} | {art:<30} | {str(opt):<12} | {cost_s:>10}"
                f" | {mrp_s:>10} | {margin_s:>7} | {str(st_pct):>5}% | {str(soh):>7}"
                f" | {div_s:<15} | {dept_s:<20} | {desc}"
            )

        lines.append("═" * 75)
        lines.append(
            "  ▶ USE THIS BLOCK for SECTION 4.5 PRODUCT ALIGNMENT. "
            "Compute Margin% = (MRP-Cost)/MRP*100. NEVER show N/A if cost or MRP present."
        )
        lines.append(
            "  ▶ option_code from inventory_current. cost_price = RATE from vitem_data (cast to float)."
        )
        lines.append(
            "  ▶ item_description = vitem_data.ITEM_NAME (falls back to ARTICLENAME if blank)."
        )
        lines.append("═" * 75)
        return "\n".join(lines)
    except Exception as e:
        logger.warning(f"_build_product_alignment_summary failed: {e}")
        return ""


def _format_supplementary_data(supplementary_data: dict, latest_sales_date: str = "") -> str:
    """
    Format pre-fetched supplementary query results into labelled data blocks.
    Feeds: STORE INVENTORY BLOCK (Section 3 ST%/DOI), Sections 4, 5, 7.
    Each section is independently formatted; any failed query is silently skipped.
    """
    if not supplementary_data:
        return ""

    date_label = f"MTD to {latest_sales_date}" if latest_sales_date else "MTD"
    sections: list[str] = []

    # ── Store Inventory Block (ST%, DOI, SOH, net_sales, SPSF per store — feeds Section 3) ───
    store_inv = supplementary_data.get("store_inventory", {})
    if store_inv.get("data") and store_inv.get("columns"):
        data, cols = store_inv["data"], store_inv["columns"]
        total_soh    = sum(float(r.get("total_soh") or 0) for r in data)
        total_sales  = sum(float(r.get("net_sales") or 0) for r in data)
        avg_st       = sum(float(r.get("sell_thru_pct") or 0) for r in data) / max(len(data), 1)
        avg_doi_vals = [float(r.get("doi") or 0) for r in data if float(r.get("doi") or 0) > 0]
        avg_doi      = sum(avg_doi_vals) / max(len(avg_doi_vals), 1)
        # Show all stores (up to 500) — LLM needs every store to match Top10/Bottom10
        display_rows = data[:500]
        sections.append(
            f"═══ STORE INVENTORY BLOCK ({date_label}) — {len(data)} stores "
            f"| Chain SOH={total_soh:,.0f} units | Chain Net Sales=₹{total_sales:,.0f} "
            f"| Chain Avg ST%={avg_st:.1f}% | Chain Avg DOI={avg_doi:.0f}d ═══"
        )
        sections.append(
            "  ▶ USE THIS BLOCK to fill ST% and DOI columns in TOP 10 / BOTTOM 10 store tables."
        )
        sections.append(
            "  ▶ MATCH: use STORE_ID (exact) OR store_name (case-insensitive substring)."
        )
        sections.append(
            "  ▶ sell_thru_pct=ST% | doi=DOI days | total_soh=SOH units | net_sales=MTD Sales ₹"
            " | SPSF: if spsf column present use it; else net_sales÷carpet_area"
        )
        sections.append(
            "  ▶ NEVER write N/A, blank, or '–' for ST%/DOI when this block is present."
            " sell_thru_pct=0.0 is VALID — show as 0.0%, NOT N/A."
        )
        sections.append(" | ".join(str(c) for c in cols))
        sections.append("-" * 80)
        for row in display_rows:
            sections.append(" | ".join(str(row.get(c, "")) for c in cols))
        sections.append("═" * 75)

    # ── Department Breakdown ─────────────────────────────────────────────────
    dept = supplementary_data.get("dept", {})
    if dept.get("data") and dept.get("columns"):
        data, cols = dept["data"], dept["columns"]
        total = sum(float(r.get("net_sales_amount") or 0) for r in data)
        display_rows = data[:12]  # Cap: top 10 + 2 buffer
        sections.append(
            f"═══ DEPARTMENT BREAKDOWN ({date_label}) — {len(data)} depts | Chain total ₹{total:,.0f} ═══"
        )
        sections.append(
            "  Columns: net_sales_amount(₹) | total_qty | bill_count | discount_pct(%) | article_count"
            " | total_soh(SOH units) | sell_thru_pct(ST%) | doi(days)"
        )
        sections.append(
            "  ATV = net_sales_amount / bill_count | UPT = total_qty / bill_count (compute inline)"
        )
        sections.append(" | ".join(str(c) for c in cols))
        sections.append("-" * 80)
        for row in display_rows:
            sections.append(" | ".join(str(row.get(c, "")) for c in cols))
        sections.append(
            "  ↑ Use for SECTION 4 TOP 10 DEPARTMENTS (sorted DESC) and "
            "BOTTOM 10 DEPARTMENTS (sorted ASC by net_sales). "
            "sell_thru_pct and doi are PRE-COMPUTED — never N/A."
        )
        sections.append("═" * 75)

    # ── Article Breakdown ────────────────────────────────────────────────────
    articles = supplementary_data.get("articles", {})
    if articles.get("data") and articles.get("columns"):
        data, cols = articles["data"], articles["columns"]
        display_rows = data[:12]  # Cap: top 10 + 2 buffer
        sections.append(
            f"═══ ARTICLE BREAKDOWN — TOP {len(data)} by Net Sales ({date_label}) ═══"
        )
        sections.append(
            "  Columns: ICODE | ARTICLENAME | DIVISION | SECTION | DEPARTMENT"
            " | STYLE_OR_PATTERN(Pattern) | SIZE | COLOR | net_sales_amount(₹)"
            " | total_qty(Qty) | bill_count | avg_mrp(MRP ₹/unit)"
            " | total_soh(SOH) | sell_thru_pct(ST%) | doi(DOI days)"
        )
        sections.append(
            "  ALL of avg_mrp, total_soh, sell_thru_pct, doi are PRE-COMPUTED — NEVER show N/A for these."
        )
        sections.append(" | ".join(str(c) for c in cols))
        sections.append("-" * 80)
        for row in display_rows:
            sections.append(" | ".join(str(row.get(c, "")) for c in cols))
        sections.append(
            "  ↑ Use top 10 rows for SECTION 4 TOP 10 ARTICLES. "
            "avg_mrp = MRP column. total_qty = Qty column. sell_thru_pct = ST%."
        )
        sections.append("═" * 75)

    # ── Bottom Articles (slowest movers) ────────────────────────────────────
    articles_btm = supplementary_data.get("articles_bottom", {})
    if articles_btm.get("data") and articles_btm.get("columns"):
        data, cols = articles_btm["data"], articles_btm["columns"]
        display_rows = data[:12]  # Cap: bottom 10 + 2 buffer
        sections.append(
            f"═══ ARTICLE BREAKDOWN — BOTTOM {len(data)} SLOWEST MOVERS ({date_label}) ═══"
        )
        sections.append(
            "  Sorted ASC = worst sellers first. Same columns as top articles:"
            " total_soh(SOH) | sell_thru_pct(ST%) | doi(DOI days) | avg_mrp(MRP) — ALL PRE-COMPUTED."
        )
        sections.append(" | ".join(str(c) for c in cols))
        sections.append("-" * 80)
        for row in display_rows:
            sections.append(" | ".join(str(row.get(c, "")) for c in cols))
        sections.append(
            "  ↑ Use for SECTION 4 BOTTOM 10 ARTICLES. SOH, DOI, ST%, MRP are pre-computed."
        )
        sections.append("═" * 75)

    # ── Peak Hours ───────────────────────────────────────────────────────────
    peak = supplementary_data.get("peak_hours", {})
    if peak.get("data") and peak.get("columns"):
        data, cols = peak["data"], peak["columns"]
        # Use the existing peak hours summary builder for chain-level aggregation
        try:
            peak_summary = _build_peak_hours_summary(data, cols)
            if peak_summary:
                sections.append(peak_summary)
            else:
                raise ValueError("empty summary")
        except Exception:
            # Fallback: raw table
            sections.append(
                f"═══ STORE PEAK HOURS — {latest_sales_date} "
                f"({len(data)} store-hour rows) ═══"
            )
            sections.append(
                "  Format: hour=HH (0-23), txn_count=bills, unique_customers=mobile uniques"
            )
            sections.append(" | ".join(str(c) for c in cols))
            sections.append("-" * 80)
            for row in data[:300]:
                sections.append(" | ".join(str(row.get(c, "")) for c in cols))
            sections.append("═" * 75)

    # ── Top 7 Highest MRP ────────────────────────────────────────────────────
    top_mrp = supplementary_data.get("top_mrp", {})
    if top_mrp.get("data") and top_mrp.get("columns"):
        data, cols = top_mrp["data"], top_mrp["columns"]
        sections.append(
            f"═══ TOP 7 HIGHEST MRP ARTICLES ({date_label}) ═══"
        )
        sections.append(
            "  unit_mrp = MRP (₹/unit, from vitem_data) | net_sales_amount = Revenue (₹)"
            " | total_qty = Qty sold | bill_count = Bills"
            " | total_soh = SOH | sell_thru_pct = ST% | doi = DOI (days)"
        )
        sections.append(
            "  ALL of unit_mrp, total_qty, total_soh, sell_thru_pct, doi are PRE-COMPUTED — NEVER N/A."
            " total_qty IS the Qty column — use it directly."
        )
        sections.append(" | ".join(str(c) for c in cols))
        sections.append("-" * 80)
        for row in data[:7]:
            sections.append(" | ".join(str(row.get(c, "")) for c in cols))
        sections.append(
            "  ↑ Use for SECTION 5 TOP 7 HIGHEST SELLING MRP. "
            "unit_mrp=MRP, total_qty=Qty, sell_thru_pct=ST%, doi=DOI."
        )
        sections.append("═" * 75)

    # ── Product Alignment ────────────────────────────────────────────────────
    pa = supplementary_data.get("product_alignment", {})
    if pa.get("data") and pa.get("columns"):
        pa_summary = _build_product_alignment_summary(pa["data"], pa["columns"])
        if pa_summary:
            sections.append(pa_summary)

    # ── Last 30 Days Daily Trend ─────────────────────────────────────────────
    last30 = supplementary_data.get("last_30_days", {})
    if last30.get("data") and last30.get("columns"):
        data, cols = last30["data"], last30["columns"]
        total_sales_30 = sum(float(r.get("net_sales", 0) or 0) for r in data)
        total_bills_30 = sum(int(r.get("bills", 0) or 0) for r in data)
        avg_daily = total_sales_30 / max(len(data), 1)
        peak_day  = max(data, key=lambda r: float(r.get("net_sales", 0) or 0), default={})
        low_day   = min(data, key=lambda r: float(r.get("net_sales", 0) or 0), default={})
        sections.append(
            f"═══ LAST 30 DAYS DAILY TREND (to {latest_sales_date}) — {len(data)} days ═══"
        )
        sections.append(
            f"  30-Day Total: ₹{total_sales_30:,.0f} | Bills: {total_bills_30:,} | "
            f"Avg Daily: ₹{avg_daily:,.0f}"
        )
        sections.append(
            f"  Peak Day: {peak_day.get('dt','')} ₹{float(peak_day.get('net_sales',0) or 0):,.0f} | "
            f"Lowest Day: {low_day.get('dt','')} ₹{float(low_day.get('net_sales',0) or 0):,.0f}"
        )
        sections.append(
            "  USE THIS for SECTION 9 → 30-DAY OUTLOOK: compute SPSF trend, sales velocity,"
            " weekend vs weekday patterns, and project next 30 days."
        )
        sections.append("  dt | net_sales(₹) | total_qty | bills | active_stores | atv(₹) | upt")
        sections.append("-" * 80)
        for row in data:
            sections.append(" | ".join(str(row.get(c, "")) for c in cols))
        sections.append("═" * 75)

    return "\n".join(sections) if sections else ""


def _merge_store_inventory(query_result: dict, supplementary_data: dict) -> dict:
    """
    Pre-merge sell_thru_pct, doi, total_soh, net_sales from the store_inventory
    supplementary block into every matching row of query_result.

    Join key: STORE_ID (String, exact) → fallback: SHRTNAME/store_name (upper, substring).
    After merge, LLM reads ST%/DOI directly from the main data table — no cross-block lookup.
    Returns a new query_result dict (original unchanged).
    """
    if not query_result.get("data"):
        return query_result

    store_inv = (supplementary_data or {}).get("store_inventory", {})
    inv_data  = store_inv.get("data", [])
    if not inv_data:
        return query_result

    # Build lookup: {store_id_str: payload}
    id_lookup: dict = {}
    name_lookup: dict = {}
    for row in inv_data:
        sid   = str(row.get("STORE_ID", row.get("store_id", ""))).strip()
        sname = str(row.get("store_name", "")).strip().upper()
        payload = {
            "sell_thru_pct": row.get("sell_thru_pct"),
            "doi":           row.get("doi"),
            "total_soh":     row.get("total_soh"),
            "inv_net_sales": row.get("net_sales"),
        }
        if sid:
            id_lookup[sid] = payload
        if sname:
            name_lookup[sname] = payload

    if not id_lookup:
        return query_result

    enriched = []
    cols = list(query_result.get("columns", []))
    col_lower_set = {c.lower() for c in cols}

    for row in query_result["data"]:
        row = dict(row)
        row_lower = {k.lower(): v for k, v in row.items()}

        # 1. Match by STORE_ID (most reliable)
        sid = str(row_lower.get("store_id", "")).strip()
        inv = id_lookup.get(sid)

        # 2. Fallback: match by SHRTNAME / store_name (case-insensitive exact)
        if not inv:
            shrt = str(row_lower.get("shrtname", row_lower.get("store_name", ""))).strip().upper()
            if shrt:
                inv = name_lookup.get(shrt)
                # 3. Fallback: substring match (handles abbreviations)
                if not inv:
                    for name_key, name_payload in name_lookup.items():
                        if shrt in name_key or name_key in shrt:
                            inv = name_payload
                            break

        if inv:
            for k, v in inv.items():
                if k not in col_lower_set:
                    row[k] = v

        enriched.append(row)

    # Add merged columns to column list (if not already present)
    for col in ["sell_thru_pct", "doi", "total_soh"]:
        if col not in col_lower_set:
            # Only add if at least one row got enriched with this col
            if any(col in r for r in enriched):
                cols.append(col)

    result = dict(query_result)
    result["data"] = enriched
    result["columns"] = cols
    matched = sum(1 for r in enriched if "sell_thru_pct" in r)
    logger.info(f"store_inventory pre-merge: {matched}/{len(enriched)} store rows enriched with ST%/DOI")
    return result


def _format_date_period_header(context: dict) -> str:
    """Compact FY period label for the prompt header."""
    period  = context.get("date_period", "MTD")
    fy_lbl  = context.get("fy_label", "")
    latest  = context.get("latest_sales_date", "")

    if period == "YTD":
        return (f"Period: YTD {fy_lbl} ({context.get('fy_start','')} → {latest}) "
                f"| FY Week {context.get('fy_week_no','')} | Days: {context.get('days_elapsed_fy','')}")
    elif period == "MTD":
        return (f"Period: MTD ({context.get('mtd_start','')} → {latest}) "
                f"| Days: {context.get('days_elapsed_mtd','')}")
    elif period == "WTD":
        return (f"Period: WTD ({context.get('wtd_start','')} → {latest}) "
                f"| Days: {context.get('days_elapsed_wtd','')}")
    elif period == "WEEK_NO":
        return f"Period: {context.get('week_label','Week')} ({context.get('week_start','')}→{context.get('week_end','')})"
    elif period == "LTL":
        return (f"Period: LTL — {context.get('ltl_current_label','Current')} "
                f"vs {context.get('ltl_prior_label','Prior')}")
    elif period == "QTD":
        return f"Period: QTD {fy_lbl} | FY Week {context.get('fy_week_no','')}"
    elif period == "TILL_DATE":
        return f"Period: As on Date — {context.get('target_date', latest)}"
    return f"Period: {latest} | FY: {fy_lbl}"


def build_analysis_prompt(
    query: str,
    context: dict,
    query_result: dict,
    kpi_results: dict = None,
    supplementary_data: dict = None,
) -> tuple[str, str]:
    """
    Build (system_prompt, user_prompt) for final LLM analytical response.
    Includes anomaly detection output, UPT, data validation context.
    """
    intent = context.get("intent", {})
    kpi_formulas = context.get("kpi_formulas", {})
    chat_history = context.get("chat_history", [])
    latest_sales_date = context.get("latest_sales_date", "")

    # Pre-merge ST%/DOI/SOH into main query result from store_inventory supplementary data.
    # This eliminates the need for LLM cross-block lookup — values appear directly in DATA RETRIEVED.
    query_result = _merge_store_inventory(query_result, supplementary_data)

    # Format data table
    data_section = _format_data(query_result, latest_sales_date=latest_sales_date)

    # Format KPI formulas
    formula_lines = "\n".join(f"  {k}: {v}" for k, v in kpi_formulas.items()) if kpi_formulas else ""

    # Format history
    history_text = format_history_for_prompt(chat_history)

    # Format anomaly section
    anomaly_text = _format_anomalies(kpi_results)

    # Build KPI availability + summary block
    kpi_sections_text = _build_kpi_sections(kpi_results) if kpi_results else ""

    # Data freshness line — enriched with FY period context
    freshness = ""
    if latest_sales_date:
        period_header = _format_date_period_header(context)
        fy_lbl = context.get("fy_label", "")
        freshness = (
            f"Financial Year: {fy_lbl}\n"
            f"{period_header}\n"
            f"Data as of: {latest_sales_date}"
        )

    # Build system prompt
    system = RIECT_SYSTEM_PROMPT + ANALYTICAL_SYSTEM_ADDENDUM

    # Build user prompt
    parts = []

    if history_text:
        parts.append(history_text)
        parts.append("")

    if formula_lines:
        parts.append(f"RIECT KPI Formulas:\n{formula_lines}")
        parts.append("")

    if freshness:
        parts.append(freshness)
        parts.append("")

    if data_section:
        parts.append(f"DATA RETRIEVED:\n{data_section}")
        parts.append("")

    # Supplementary data: dept breakdown, articles, peak hours, top MRP
    supp_section = _format_supplementary_data(supplementary_data, latest_sales_date)
    if supp_section:
        parts.append(f"SUPPLEMENTARY DATA (use for Sections 4, 5, 7):\n{supp_section}")
        parts.append("")

    if kpi_sections_text:
        parts.append(f"KPI AVAILABILITY MAP:\n{kpi_sections_text}")
        parts.append("")

    if anomaly_text:
        parts.append(f"ANOMALY DETECTION OUTPUT:\n{anomaly_text}")
        parts.append("")

    intent_label = intent.get("intent", "general_retail")
    kpi_types = intent.get("kpi_types", [])
    if kpi_types:
        parts.append(f"Analysis focus: {intent_label} | KPIs: {', '.join(kpi_types)}")
        parts.append("")

    # Zone filter context — tells the LLM which zone was requested so it can verify rows
    zone_filter = context.get("zone_filter", {})
    if zone_filter:
        zone_val = zone_filter.get("zone", "")
        parts.append(
            f"⚠ ZONE FILTER ACTIVE: User requested '{zone_val}' zone only.\n"
            f"CRITICAL: Only include stores where ZONE = '{zone_val}' in ALL tables and outputs.\n"
            f"If any store row has ZONE ≠ '{zone_val}' → EXCLUDE it immediately.\n"
            f"NEVER write 'data not available for {zone_val} stores' — show ALL matching rows."
        )
        parts.append("")

    parts.append(f"USER QUESTION: {query}")
    parts.append("")
    parts.append(
        "Detect the query type (RAW DATA / KPI ANALYSIS / INSIGHT-STRATEGY) and apply the correct FORMAT.\n"
        "MANDATORY — THESE RULES OVERRIDE EVERYTHING:\n"
        "\n"
        "ROW COUNTS (NON-NEGOTIABLE):\n"
        "- Top 10 = exactly 10 rows | Bottom 10 = exactly 10 rows | Top 7 = exactly 7 rows.\n"
        "- NEVER stop at 3, 5, or any fewer. COMPLETE EVERY TABLE before moving to next section.\n"
        "\n"
        "COLUMN VALUES — NEVER SHOW N/A WHEN DATA IS PRESENT:\n"
        "- ATV: compute net_sales_amount ÷ bill_count inline — NEVER N/A if both columns exist\n"
        "- UPT: compute total_qty ÷ bill_count inline — NEVER N/A if both columns exist\n"
        "- ST% (stores): the sell_thru_pct column IS PRE-MERGED into DATA RETRIEVED rows — use directly\n"
        "  → NEVER look up from STORE INVENTORY BLOCK — it is already in the main data table\n"
        "  → If sell_thru_pct = 0.0 → show '0.0%', NOT N/A or '–'\n"
        "  → If sell_thru_pct is absent from a row → write 'Inv N/A' for that store only\n"
        "- DOI (stores): the doi column IS PRE-MERGED into DATA RETRIEVED rows — use directly\n"
        "  → doi=0 means zero inventory (all sold) → show '0d', NOT N/A\n"
        "  → If doi is absent from a row → write 'Inv N/A' for that store only\n"
        "- ST% (dept/articles): sell_thru_pct column is PRE-COMPUTED — use directly, NEVER N/A\n"
        "- DOI (dept/articles): doi column is PRE-COMPUTED — use directly, NEVER N/A\n"
        "- SOH: total_soh column is PRE-COMPUTED — use directly, NEVER N/A\n"
        "- MRP: avg_mrp (articles/dept) or unit_mrp (top MRP section) — NEVER N/A if column present\n"
        "- Qty: total_qty column — NEVER N/A if column present\n"
        "- Unique Customers (Mobile): unique_customers column — show 0 if zero, NEVER N/A\n"
        "- Peak Hours UPT: total_qty ÷ txn_count per store — COMPUTE INLINE, NEVER N/A\n"
        "- Peak Hours ATV: net_sales_amount ÷ txn_count per store — COMPUTE INLINE, NEVER N/A\n"
        "\n"
        "SUPPLEMENTARY DATA:\n"
        "- STORE INVENTORY BLOCK → reference for chain-level SOH/ST%/DOI summaries only\n"
        "  (per-store ST%/DOI are already merged into DATA RETRIEVED — no lookup needed)\n"
        "- DEPARTMENT BREAKDOWN → Section 4 Dept tables\n"
        "- ARTICLE BREAKDOWN → Section 4 Article tables (TOP and BOTTOM)\n"
        "- TOP 7 HIGHEST MRP → Section 5\n"
        "- PEAK HOURS → Section 7\n"
        "All Sections 4, 5, 7 MUST use supplementary data. Section 3 ST%/DOI from DATA RETRIEVED.\n"
        "\n"
        "SECTION 9 (EXECUTIVE CONCLUSION) IS MANDATORY:\n"
        "- ALWAYS end every FORMAT B / FORMAT C response with Section 9.\n"
        "- Include: Chain Health Snapshot | Top 10 Wins | Top 10 Critical Risks | Top 5 Actions | 30-Day Outlook.\n"
        "\n"
        "INSIGHTS — MINIMUM DEPTH REQUIRED:\n"
        "- Every section: minimum 3 insight bullets with specific numbers, store/dept/article names.\n"
        "- Section 3 Top 3 stores: 5-bullet insight block each (What's Working, Risk, Maintain, Replicate, Outlook).\n"
        "- Section 8 Priority Actions: minimum 5 per priority tier (5 Critical, 5 High, 5 Medium).\n"
        "\n"
        "GENERAL:\n"
        "- No paragraphs. Tables, bullets, numbered lists only.\n"
        "- Every number: cite source (store name, date, column). Every P1/P2: exact value + risk flag.\n"
        "- Every action: WHO + WHAT + HOW MUCH (gap) + WHEN.\n"
        "- Use CHAIN TOTALS for net sales/qty. KPI CHAIN SUMMARIES for chain averages.\n"
        "- If a column is genuinely absent from ALL data blocks: write 'Col N/A' once in header."
    )

    user_prompt = "\n".join(parts)
    return system, user_prompt


def _format_anomalies(kpi_results: dict) -> str:
    """Format anomaly detection results for prompt injection."""
    if not kpi_results:
        return ""
    anomaly_result = kpi_results.get("anomalies", {})
    if not anomaly_result or not anomaly_result.get("anomalies"):
        return ""
    try:
        from riect.kpi_engine.anomaly_engine import format_anomalies_for_prompt
        return format_anomalies_for_prompt(anomaly_result)
    except Exception:
        return ""


def _build_kpi_sections(kpi_results: dict) -> str:
    """
    Build a KPI AVAILABILITY + SUMMARY block for the LLM prompt.
    Only includes KPIs that are actually available (available=True) in the current result.
    Groups by category. Provides mean/target/P1-count per KPI for LLM guidance.
    """
    if not kpi_results:
        return ""

    availability = kpi_results.get("kpi_availability", {})
    if not availability:
        return ""

    # Category → list of (kpi_key, summary_dict) for available KPIs
    category_map: dict = {}
    for kpi_key, avail in availability.items():
        if not avail:
            continue
        meta = KPI_REGISTRY.get(kpi_key, {})
        category = meta.get("category", "Other")
        label    = meta.get("label", kpi_key.upper())

        # Grab summary from kpi_results — map kpi_key to result key
        result_key = kpi_key
        if kpi_key == "mobile_penetration":
            result_key = "mobile_pct"
        engine_res = kpi_results.get(result_key, {})
        summary    = engine_res.get("summary", {}) if isinstance(engine_res, dict) else {}

        category_map.setdefault(category, []).append((label, kpi_key, summary))

    if not category_map:
        return ""

    lines = [
        "═══ KPI AVAILABILITY MAP ═══════════════════════════════════════════════════",
        "  (Only KPIs listed below are computable from the current SQL result.)",
        "  Sections NOT listed here = required columns absent → SKIP those sections entirely.",
        "",
    ]

    for category in sorted(category_map.keys()):
        items = category_map[category]
        lines.append(f"  ▶ {category}")
        for label, kpi_key, summary in items:
            if summary:
                # Build a compact summary line from whatever keys the engine returned
                summary_parts = []
                for sk, sv in list(summary.items())[:6]:
                    if isinstance(sv, float):
                        summary_parts.append(f"{sk}={sv:.3g}")
                    else:
                        summary_parts.append(f"{sk}={sv}")
                lines.append(f"    ✓ {label}: {' | '.join(summary_parts)}")
            else:
                lines.append(f"    ✓ {label}: available (no pre-computed summary)")

    lines.append("")
    # List unavailable KPIs so the LLM knows what to skip
    unavailable = [
        KPI_REGISTRY.get(k, {}).get("label", k.upper())
        for k, v in availability.items()
        if not v
    ]
    if unavailable:
        lines.append(
            f"  ✗ NOT AVAILABLE (skip): {', '.join(unavailable[:12])}"
            + (" …" if len(unavailable) > 12 else "")
        )
    lines.append("═" * 75)
    return "\n".join(lines)


def _build_chain_totals(data: list, columns: list, latest_sales_date: str = "") -> str:
    """
    Build an explicit chain totals block for net_sales_amount, total_qty, and bill_count.
    This gives the LLM unambiguous chain-wide aggregates — preventing it from reporting
    per-store averages or confused values as the chain total.
    Only generated when net_sales_amount or total_qty is present in the result.
    """
    try:
        col_lower = [c.lower() for c in columns]

        sales_col = next((columns[i] for i, c in enumerate(col_lower) if c == "net_sales_amount"), None)
        qty_col   = next((columns[i] for i, c in enumerate(col_lower) if c in {"total_qty", "qty"}), None)
        bill_col  = next((columns[i] for i, c in enumerate(col_lower) if c in {"bill_count", "bills_count"}), None)

        if not sales_col and not qty_col:
            return ""

        chain_sales = sum(float(row.get(sales_col) or 0) for row in data) if sales_col else None
        chain_qty   = sum(float(row.get(qty_col)   or 0) for row in data) if qty_col   else None
        chain_bills = sum(float(row.get(bill_col)  or 0) for row in data) if bill_col  else None

        date_label = f"Date: {latest_sales_date}" if latest_sales_date else "Date: latest available"
        lines = [
            f"═══ CHAIN TOTALS ({date_label}) ══════════════════════════════════════════",
        ]
        if chain_sales is not None:
            lines.append(f"  Chain Net Sales Amount : ₹{chain_sales:,.2f}  ← USE THIS for total chain net sales")
        if chain_qty is not None:
            lines.append(f"  Chain Total Qty (units): {chain_qty:,.0f}  ← USE THIS for total chain qty sold")
        if chain_bills is not None:
            lines.append(f"  Chain Total Bills      : {chain_bills:,.0f}")
        lines.append(f"  Stores / rows reporting: {len(data)}")
        lines.append(
            "  ⚠ IMPORTANT: Use CHAIN TOTALS above — NOT avg or per-store values — for chain-wide summary."
        )
        lines.append("═══════════════════════════════════════════════════════════════════════════")
        return "\n".join(lines) + "\n"
    except Exception:
        return ""


def _format_data(query_result: dict, latest_sales_date: str = "") -> str:
    """Format query result as compact text table for LLM.
    Injects KPI chain summaries (SPSF, DOI, Sell-Through, UPT) and per-store cross-KPI view.
    """
    if not query_result or "error" in query_result:
        if query_result and "error" in query_result:
            return f"[Query error: {query_result['error']}]"
        return "[No data available]"

    data = query_result.get("data", [])
    columns = query_result.get("columns", [])
    row_count = query_result.get("row_count", 0)

    if not data or not columns:
        return "[Empty result set]"

    # KPI columns to skip from generic stats (have dedicated chain summaries)
    KPI_SKIP_COLS = {"spsf", "sell_thru_pct", "sell_thru_pct_display", "doi", "upt",
                     "days_of_cover", "sell_thru_method", "hour"}

    # ── Chain Totals (explicit net_sales + qty anchor) ───────────────────────
    chain_totals = _build_chain_totals(data, columns, latest_sales_date)

    # ── KPI Chain Summaries ──────────────────────────────────────────────────
    spsf_summary      = _build_spsf_chain_summary(data, columns)
    doi_summary       = _build_doi_chain_summary(data, columns)
    sell_thru_summary = _build_sell_thru_chain_summary(data, columns)
    upt_summary       = _build_upt_chain_summary(data, columns)
    peak_hrs_summary  = _build_peak_hours_summary(data, columns)

    # ── Aggregate stats for non-KPI numeric columns ──────────────────────────
    numeric_stats = []
    try:
        for col in columns:
            if col.lower() in KPI_SKIP_COLS:
                continue
            vals = [row.get(col) for row in data if isinstance(row.get(col), (int, float))]
            if vals:
                total = sum(vals)
                avg = total / len(vals)
                numeric_stats.append(
                    f"  {col}: total={total:,.2f}, avg={avg:,.2f}, "
                    f"min={min(vals):,.2f}, max={max(vals):,.2f}, count={len(vals)}"
                )
    except Exception:
        pass

    # ── Cross-KPI per-store view (when 2+ KPIs available) ───────────────────
    cross_kpi = _build_cross_kpi_store_table(data, columns)

    # ── Data table (up to MAX rows) ──────────────────────────────────────────
    display_rows = data[:MAX_DATA_ROWS_IN_PROMPT]

    lines = []
    if chain_totals:
        lines.append(chain_totals)
    for summary in [spsf_summary, doi_summary, sell_thru_summary, upt_summary, peak_hrs_summary]:
        if summary:
            lines.append(summary)

    if cross_kpi:
        lines.append(cross_kpi)

    lines.append(" | ".join(str(c) for c in columns))
    lines.append("-" * min(120, len(lines[-1])))
    for row in display_rows:
        lines.append(" | ".join(str(row.get(c, "")) for c in columns))

    if row_count > MAX_DATA_ROWS_IN_PROMPT:
        lines.append(
            f"\n[NOTE: Showing {MAX_DATA_ROWS_IN_PROMPT} of {row_count} total rows. "
            f"Aggregate statistics below cover ALL {row_count} rows.]"
        )

    summary_parts = [f"Total rows: {row_count} | Execution: {query_result.get('execution_time_ms', 0)}ms"]
    if numeric_stats:
        summary_parts.append("Aggregate statistics — raw columns (all rows):\n" + "\n".join(numeric_stats))

    lines.append("\n" + "\n".join(summary_parts))
    return "\n".join(lines)


def _build_spsf_chain_summary(data: list, columns: list) -> str:
    """
    Build an explicit SPSF Chain Summary block when 'spsf' column is present.
    This prevents the LLM from mis-computing chain average from net_sales/floor_sqft.
    """
    try:
        from config import SPSF_THRESHOLDS, MIN_SQFT_FOR_SPSF

        col_lower = [c.lower() for c in columns]
        if "spsf" not in col_lower:
            return ""

        spsf_col = columns[col_lower.index("spsf")]
        sqft_col = next((columns[i] for i, c in enumerate(col_lower) if c == "floor_sqft"), None)
        shrt_col = next((columns[i] for i, c in enumerate(col_lower)
                         if c in {"shrtname", "store_name", "storename"}), None)

        # Collect valid spsf values — filter out stores with tiny sqft (kiosks/data errors)
        valid, excluded = [], []
        for row in data:
            spsf_val = row.get(spsf_col)
            sqft_val = row.get(sqft_col, 0) if sqft_col else MIN_SQFT_FOR_SPSF
            if not isinstance(spsf_val, (int, float)) or spsf_val <= 0:
                continue
            if sqft_val and sqft_val < MIN_SQFT_FOR_SPSF:
                store = row.get(shrt_col, "?") if shrt_col else "?"
                excluded.append(f"{store}({sqft_val}sqft)")
                continue
            valid.append((spsf_val, row.get(shrt_col, "") if shrt_col else ""))

        if not valid:
            return ""

        vals = [v for v, _ in valid]
        chain_avg = sum(vals) / len(vals)
        chain_min = min(vals)
        chain_max = max(vals)
        p1 = sum(1 for v in vals if v < SPSF_THRESHOLDS["P1"])
        p2 = sum(1 for v in vals if SPSF_THRESHOLDS["P1"] <= v < SPSF_THRESHOLDS["P2"])
        p3 = sum(1 for v in vals if SPSF_THRESHOLDS["P2"] <= v < SPSF_THRESHOLDS["P3"])
        on_target = sum(1 for v in vals if v >= SPSF_THRESHOLDS["target"])

        # Top 3 and bottom 3 stores by SPSF
        sorted_stores = sorted(valid, key=lambda x: x[0], reverse=True)
        top3 = " | ".join(f"{n}=₹{v:.1f}" for v, n in sorted_stores[:3] if n)
        bot3 = " | ".join(f"{n}=₹{v:.1f}" for v, n in sorted_stores[-3:] if n)

        lines = [
            "═══ SPSF CHAIN SUMMARY (MTD — Month-to-Date) ═════════════════════════════",
            f"  Chain avg MTD SPSF : ₹{chain_avg:.2f}/sqft  ← USE THIS for chain average",
            f"  Target (monthly)   : ₹{SPSF_THRESHOLDS['target']}/sqft",
            f"  Min / Max          : ₹{chain_min:.2f} / ₹{chain_max:.2f}/sqft",
            f"  P1 Critical (<₹{SPSF_THRESHOLDS['P1']}): {p1} stores  |  "
            f"P2 High (<₹{SPSF_THRESHOLDS['P2']}): {p2}  |  "
            f"P3 Medium (<₹{SPSF_THRESHOLDS['P3']}): {p3}  |  "
            f"On Target (≥₹{SPSF_THRESHOLDS['target']}): {on_target}",
            f"  Stores in SPSF analysis: {len(vals)}",
        ]
        if top3:
            lines.append(f"  Top 3  : {top3}")
        if bot3:
            lines.append(f"  Bottom 3: {bot3}")
        if excluded:
            lines.append(f"  Excluded (<{MIN_SQFT_FOR_SPSF}sqft): {', '.join(excluded[:5])}"
                         + (f" +{len(excluded)-5} more" if len(excluded) > 5 else ""))
        lines.append(
            "  ⚠ DO NOT use net_sales_amount as SPSF. The 'spsf' column = authentic ₹/sqft values."
        )
        lines.append("═══════════════════════════════════════════════════════════════════════════")
        return "\n".join(lines) + "\n"

    except Exception:
        return ""


def _build_doi_chain_summary(data: list, columns: list) -> str:
    """
    Build DOI Chain Summary block when 'doi' column is present.
    Prevents LLM from mis-reading raw avg or computing incorrectly.
    """
    try:
        from config import DOI_THRESHOLDS

        col_lower = [c.lower() for c in columns]
        if "doi" not in col_lower:
            return ""

        doi_col = columns[col_lower.index("doi")]
        shrt_col = next((columns[i] for i, c in enumerate(col_lower)
                         if c in {"shrtname", "store_name", "storename", "store_code"}), None)

        vals = []
        for row in data:
            v = row.get(doi_col)
            if isinstance(v, (int, float)) and v >= 0:
                store = row.get(shrt_col, "") if shrt_col else ""
                vals.append((v, store))

        if not vals:
            return ""

        doi_vals = [v for v, _ in vals]
        chain_avg = sum(doi_vals) / len(doi_vals)
        p1 = sum(1 for v in doi_vals if v > DOI_THRESHOLDS["P1"])
        p2 = sum(1 for v in doi_vals if DOI_THRESHOLDS["P2"] < v <= DOI_THRESHOLDS["P1"])
        p3 = sum(1 for v in doi_vals if DOI_THRESHOLDS["P3"] < v <= DOI_THRESHOLDS["P2"])
        ok = sum(1 for v in doi_vals if v <= DOI_THRESHOLDS["P3"])

        sorted_stores = sorted(vals, key=lambda x: x[0])
        top3 = " | ".join(f"{n}={v:.0f}d" for v, n in sorted_stores[:3] if n)
        bot3 = " | ".join(f"{n}={v:.0f}d" for v, n in sorted_stores[-3:] if n)

        lines = [
            "═══ DOI CHAIN SUMMARY ════════════════════════════════════════════════════",
            f"  Chain avg DOI  : {chain_avg:.1f} days  ← USE THIS for chain average",
            f"  Target         : {DOI_THRESHOLDS['target']} days",
            f"  P1 Critical (>{DOI_THRESHOLDS['P1']}d): {p1}  |  "
            f"P2 High (>{DOI_THRESHOLDS['P2']}d): {p2}  |  "
            f"P3 Watch (>{DOI_THRESHOLDS['P3']}d): {p3}  |  "
            f"On Target (≤{DOI_THRESHOLDS['P3']}d): {ok}",
            f"  Stores in DOI analysis: {len(vals)}",
        ]
        if top3:
            lines.append(f"  Lowest DOI (freshest): {top3}")
        if bot3:
            lines.append(f"  Highest DOI (overstock): {bot3}")
        lines.append(
            "  Formula: (SOH + GIT_QTY) ÷ Avg Daily Sale Qty (30d rolling)"
        )
        lines.append("═══════════════════════════════════════════════════════════════════════════")
        return "\n".join(lines) + "\n"

    except Exception:
        return ""


def _build_sell_thru_chain_summary(data: list, columns: list) -> str:
    """
    Build Sell-Through Chain Summary when 'sell_thru_pct' column is present.
    Shows chain average, threshold breaches, and which formula variant was used.
    """
    try:
        from config import SELL_THRU_THRESHOLDS

        col_lower = [c.lower() for c in columns]
        if "sell_thru_pct" not in col_lower:
            return ""

        st_col = columns[col_lower.index("sell_thru_pct")]
        method_col = next((columns[i] for i, c in enumerate(col_lower)
                           if c == "sell_thru_method"), None)
        shrt_col = next((columns[i] for i, c in enumerate(col_lower)
                         if c in {"shrtname", "store_name", "storename", "store_code",
                                  "icode", "articlename", "category"}), None)

        vals = []
        method_used = ""
        for row in data:
            v = row.get(st_col)
            if isinstance(v, (int, float)) and 0 <= v <= 1:
                store = row.get(shrt_col, "") if shrt_col else ""
                vals.append((v, store))
            if method_col and not method_used:
                method_used = str(row.get(method_col, ""))

        if not vals:
            return ""

        st_vals = [v for v, _ in vals]
        chain_avg = sum(st_vals) / len(st_vals) * 100
        t = SELL_THRU_THRESHOLDS
        p1 = sum(1 for v in st_vals if v < t["P1"])
        p2 = sum(1 for v in st_vals if t["P1"] <= v < t["P2"])
        p3 = sum(1 for v in st_vals if t["P2"] <= v < t["P3"])
        ok = sum(1 for v in st_vals if v >= t["P3"])

        sorted_stores = sorted(vals, key=lambda x: x[0], reverse=True)
        top3 = " | ".join(f"{n}={v*100:.1f}%" for v, n in sorted_stores[:3] if n)
        bot3 = " | ".join(f"{n}={v*100:.1f}%" for v, n in sorted_stores[-3:] if n)

        method_label = ""
        if "A:" in method_used:
            method_label = "  Formula: Variant A — SALE_QTY / (OPEN_QTY + IN_QTY)"
        elif "B:" in method_used:
            method_label = "  Formula: Variant B — SALE_QTY / (SALE_QTY + SOH) [opening stock unavailable]"

        lines = [
            "═══ SELL-THROUGH CHAIN SUMMARY ══════════════════════════════════════════",
            f"  Chain avg ST%  : {chain_avg:.1f}%  ← USE THIS for chain average",
            f"  Target         : {int(t['target']*100)}%",
            f"  P1 Critical (<{int(t['P1']*100)}%): {p1}  |  "
            f"P2 High (<{int(t['P2']*100)}%): {p2}  |  "
            f"P3 Watch (<{int(t['P3']*100)}%): {p3}  |  "
            f"On Target (≥{int(t['P3']*100)}%): {ok}",
            f"  Items in ST analysis: {len(vals)}",
        ]
        if method_label:
            lines.append(method_label)
        if top3:
            lines.append(f"  Best ST%: {top3}")
        if bot3:
            lines.append(f"  Worst ST%: {bot3}")
        lines.append("═══════════════════════════════════════════════════════════════════════════")
        return "\n".join(lines) + "\n"

    except Exception:
        return ""


def _build_upt_chain_summary(data: list, columns: list) -> str:
    """
    Build UPT Chain Summary when 'upt' column present, or derive from qty + bill_count.
    UPT = Total Qty ÷ COUNT(DISTINCT BILLNO) per store.
    """
    try:
        from config import UPT_THRESHOLDS

        col_lower = [c.lower() for c in columns]
        shrt_col = next((columns[i] for i, c in enumerate(col_lower)
                         if c in {"shrtname", "store_name", "storename"}), None)

        # Try pre-computed upt column first
        if "upt" in col_lower:
            upt_col = columns[col_lower.index("upt")]
            vals = []
            for row in data:
                v = row.get(upt_col)
                if isinstance(v, (int, float)) and v > 0:
                    store = row.get(shrt_col, "") if shrt_col else ""
                    vals.append((v, store))
        else:
            # Derive UPT from qty + bill_count columns
            qty_col = next((c for c in ["qty", "total_qty", "sale_qty"] if c in col_lower), None)
            bill_col = next((c for c in ["bill_count", "bills_count", "billno_count"] if c in col_lower), None)
            if not qty_col or not bill_col:
                return ""
            qty_col = columns[col_lower.index(qty_col)]
            bill_col = columns[col_lower.index(bill_col)]
            vals = []
            for row in data:
                qty = row.get(qty_col)
                bills = row.get(bill_col)
                if isinstance(qty, (int, float)) and isinstance(bills, (int, float)) and bills > 0:
                    upt = qty / bills
                    store = row.get(shrt_col, "") if shrt_col else ""
                    vals.append((round(upt, 2), store))

        if not vals or len(vals) < 2:
            return ""

        upt_vals = [v for v, _ in vals]
        chain_avg = sum(upt_vals) / len(upt_vals)
        t = UPT_THRESHOLDS
        p1 = sum(1 for v in upt_vals if v < t["P1"])
        p2 = sum(1 for v in upt_vals if t["P1"] <= v < t["P2"])
        p3 = sum(1 for v in upt_vals if t["P2"] <= v < t["P3"])
        ok = sum(1 for v in upt_vals if v >= t["target"])

        sorted_stores = sorted(vals, key=lambda x: x[0], reverse=True)
        top3 = " | ".join(f"{n}={v:.2f}" for v, n in sorted_stores[:3] if n)
        bot3 = " | ".join(f"{n}={v:.2f}" for v, n in sorted_stores[-3:] if n)

        lines = [
            "═══ UPT CHAIN SUMMARY ════════════════════════════════════════════════════",
            f"  Chain avg UPT  : {chain_avg:.2f} items/bill  ← USE THIS for chain average",
            f"  Target         : {t['target']} items/bill",
            f"  P1 Critical (<{t['P1']}): {p1} stores  |  "
            f"P2 High (<{t['P2']}): {p2}  |  "
            f"P3 Watch (<{t['P3']}): {p3}  |  "
            f"On Target (≥{t['target']}): {ok}",
            f"  Stores in UPT analysis: {len(vals)}",
        ]
        if top3:
            lines.append(f"  Best UPT: {top3}")
        if bot3:
            lines.append(f"  Lowest UPT: {bot3}")
        lines.append(
            "  Formula: SUM(QTY) ÷ COUNT(DISTINCT BILLNO) per store"
        )
        lines.append("═══════════════════════════════════════════════════════════════════════════")
        return "\n".join(lines) + "\n"

    except Exception:
        return ""


def _build_cross_kpi_store_table(data: list, columns: list) -> str:
    """
    Build per-store cross-KPI table when 2+ KPI types are present.
    Shows: STORE | SPSF | ST% | DOI | UPT — all in one row per store.
    Only generated if at least SPSF + one other KPI is available.
    """
    try:
        col_lower = [c.lower() for c in columns]

        # Detect bill_count and net_sales columns (needed for UPT and ATV)
        bill_col_name = next((c for c in ["bill_count", "bills_count", "billno_count"] if c in col_lower), None)
        bill_avail = bill_col_name is not None
        sales_col = next((columns[i] for i, c in enumerate(col_lower) if c == "net_sales_amount"), None)

        # Check which KPIs are available
        has_spsf = "spsf" in col_lower
        has_st   = "sell_thru_pct" in col_lower
        has_doi  = "doi" in col_lower
        has_atv  = sales_col is not None and bill_avail

        # Need qty + bill_count to compute UPT if not pre-computed
        has_upt = "upt" in col_lower
        if not has_upt:
            qty_avail = any(c in col_lower for c in ["qty", "total_qty"])
            has_upt = qty_avail and bill_avail

        # Only generate if at least 2 metrics available
        active_kpis = sum([has_spsf, has_st, has_doi, has_upt, has_atv])
        if active_kpis < 2:
            return ""

        # Column refs
        spsf_col  = columns[col_lower.index("spsf")] if has_spsf else None
        st_col    = columns[col_lower.index("sell_thru_pct")] if has_st else None
        doi_col   = columns[col_lower.index("doi")] if has_doi else None
        upt_col   = columns[col_lower.index("upt")] if "upt" in col_lower else None
        shrt_col  = next((columns[i] for i, c in enumerate(col_lower)
                          if c in {"shrtname", "store_name", "storename"}), None)
        qty_col   = next((columns[i] for i, c in enumerate(col_lower)
                          if c in {"qty", "total_qty", "sale_qty"}), None) if has_upt else None
        bill_col  = columns[col_lower.index(bill_col_name)] if bill_avail else None

        # Build header
        header_parts = ["Store"]
        if has_spsf:   header_parts += ["SPSF (₹/sqft)", "SPSF-P"]
        if has_atv:    header_parts.append("ATV (₹)")
        if has_st:     header_parts += ["Sell-Through%", "ST-P"]
        if has_doi:    header_parts += ["DOI (days)", "DOI-P"]
        if has_upt:    header_parts += ["UPT", "UPT-P"]

        from config import SPSF_THRESHOLDS, SELL_THRU_THRESHOLDS, DOI_THRESHOLDS, UPT_THRESHOLDS

        def spsf_p(v): return "P1" if v < SPSF_THRESHOLDS["P1"] else "P2" if v < SPSF_THRESHOLDS["P2"] else "P3" if v < SPSF_THRESHOLDS["P3"] else "OK"
        def st_p(v):   return "P1" if v < SELL_THRU_THRESHOLDS["P1"] else "P2" if v < SELL_THRU_THRESHOLDS["P2"] else "P3" if v < SELL_THRU_THRESHOLDS["P3"] else "OK"
        def doi_p(v):  return "P1" if v > DOI_THRESHOLDS["P1"] else "P2" if v > DOI_THRESHOLDS["P2"] else "P3" if v > DOI_THRESHOLDS["P3"] else "OK"
        def upt_p(v):  return "P1" if v < UPT_THRESHOLDS["P1"] else "P2" if v < UPT_THRESHOLDS["P2"] else "P3" if v < UPT_THRESHOLDS["P3"] else "OK"

        rows = []
        for row in data[:50]:  # cap at 50 stores for readability
            store = str(row.get(shrt_col, "?")) if shrt_col else "?"
            parts = [store]
            if has_spsf:
                v = row.get(spsf_col)
                if isinstance(v, (int, float)):
                    parts += [f"₹{v:.1f}", spsf_p(v)]
                else:
                    parts += ["-", "-"]
            if has_atv:
                net   = float(row.get(sales_col, 0) or 0) if sales_col else 0
                bills = float(row.get(bill_col, 0) or 0) if bill_col else 0
                atv_v = round(net / bills, 0) if bills > 0 else None
                parts.append(f"₹{int(atv_v):,}" if atv_v else "-")
            if has_st:
                v = row.get(st_col)
                if isinstance(v, (int, float)):
                    parts += [f"{v*100:.1f}%", st_p(v)]
                else:
                    parts += ["-", "-"]
            if has_doi:
                v = row.get(doi_col)
                if isinstance(v, (int, float)):
                    parts += [f"{v:.0f}d", doi_p(v)]
                else:
                    parts += ["-", "-"]
            if has_upt:
                if upt_col:
                    v = row.get(upt_col)
                else:
                    qty = row.get(qty_col, 0) or 0
                    bills = row.get(bill_col, 0) or 0
                    v = qty / bills if bills > 0 else None
                if isinstance(v, (int, float)):
                    parts += [f"{v:.2f}", upt_p(v)]
                else:
                    parts += ["-", "-"]
            rows.append(parts)

        if not rows:
            return ""

        sep = " | "
        header_line = sep.join(header_parts)
        output = [
            "═══ PER-STORE CROSS-KPI VIEW ══════════════════════════════════════════════",
            header_line,
            "-" * min(120, len(header_line)),
        ]
        for r in rows:
            output.append(sep.join(r))
        output.append("═══════════════════════════════════════════════════════════════════════════")
        return "\n".join(output) + "\n"

    except Exception:
        return ""


def _build_peak_hours_summary(data: list, columns: list) -> str:
    """
    Build Peak Hours Chain Summary when 'hour' column is present alongside txn_count/revenue.
    Aggregates across all stores to show chain-wide peak windows and per-store peak hours.
    Input: rows of (store, hour, txn_count, revenue, qty) from pos_transactional_data.
    """
    try:
        col_lower = [c.lower() for c in columns]
        if "hour" not in col_lower:
            return ""

        hour_col  = columns[col_lower.index("hour")]
        txn_col   = next((columns[i] for i, c in enumerate(col_lower)
                          if c in {"txn_count", "bill_count", "bills_count", "transaction_count"}), None)
        cust_col  = next((columns[i] for i, c in enumerate(col_lower)
                          if c in {"unique_customers", "customer_count", "mobile_count"}), None)
        rev_col   = next((columns[i] for i, c in enumerate(col_lower)
                          if c in {"revenue", "net_sales_amount", "netamt", "total_netamt"}), None)
        qty_col   = next((columns[i] for i, c in enumerate(col_lower)
                          if c in {"qty", "total_qty", "quantity"}), None)
        shrt_col  = next((columns[i] for i, c in enumerate(col_lower)
                          if c in {"shrtname", "store_name", "storename"}), None)
        zone_col  = next((columns[i] for i, c in enumerate(col_lower) if c == "zone"), None)
        region_col= next((columns[i] for i, c in enumerate(col_lower) if c == "region"), None)

        if not txn_col and not rev_col:
            return ""

        # ── Aggregate by hour (chain-wide) and by store ──────────────────────
        hour_agg: dict[int, dict] = {}   # hour → {txn, rev, qty, stores}
        store_hours: dict[str, dict[int, dict]] = {}  # store → hour → {txn, rev}

        for row in data:
            h = row.get(hour_col)
            if not isinstance(h, (int, float)):
                continue
            h = int(h)
            txn  = float(row.get(txn_col,  0) or 0) if txn_col  else 0
            cust = float(row.get(cust_col, 0) or 0) if cust_col else 0
            rev  = float(row.get(rev_col,  0) or 0) if rev_col  else 0
            qty  = float(row.get(qty_col,  0) or 0) if qty_col  else 0
            store  = str(row.get(shrt_col,   "")) if shrt_col   else ""
            zone   = str(row.get(zone_col,   "")) if zone_col   else ""
            region = str(row.get(region_col, "")) if region_col else ""

            if h not in hour_agg:
                hour_agg[h] = {"txn": 0.0, "cust": 0.0, "rev": 0.0, "qty": 0.0, "stores": set()}
            hour_agg[h]["txn"]  += txn
            hour_agg[h]["cust"] += cust
            hour_agg[h]["rev"]  += rev
            hour_agg[h]["qty"]  += qty
            if store:
                hour_agg[h]["stores"].add(store)
                if store not in store_hours:
                    store_hours[store] = {"zone": zone, "region": region, "total_qty": 0.0, "hours": {}}
                store_hours[store]["total_qty"] += qty
                if h not in store_hours[store]["hours"]:
                    store_hours[store]["hours"][h] = {"txn": 0.0, "cust": 0.0, "rev": 0.0}
                store_hours[store]["hours"][h]["txn"]  += txn
                store_hours[store]["hours"][h]["cust"] += cust
                store_hours[store]["hours"][h]["rev"]  += rev

        if not hour_agg:
            return ""

        def fmt_hr(h: int) -> str:
            return f"{h:02d}:00–{(h+1)%24:02d}:00"

        # ── Chain-wide top 3 by transactions and revenue ─────────────────────
        by_txn = sorted(hour_agg.items(), key=lambda x: x[1]["txn"], reverse=True)
        by_rev = sorted(hour_agg.items(), key=lambda x: x[1]["rev"], reverse=True)

        peak_h_txn = by_txn[0][0]
        peak_h_rev = by_rev[0][0]

        top3_txn = " | ".join(
            f"{fmt_hr(h)} — {v['txn']:,.0f} txns / {len(v['stores'])} stores"
            for h, v in by_txn[:3]
        )
        top3_rev = " | ".join(
            f"{fmt_hr(h)} — ₹{v['rev']:,.0f}"
            for h, v in by_rev[:3]
        )

        # ── Identify trading windows (consecutive peak hours) ─────────────────
        all_hours = sorted(hour_agg.keys())
        # Mark hours with txn > 50% of peak as "active"
        peak_txn = by_txn[0][1]["txn"]
        active = [h for h in all_hours if hour_agg[h]["txn"] >= peak_txn * 0.5]
        window = f"{fmt_hr(active[0])} to {fmt_hr(active[-1])}" if active else ""

        # ── Per-store peak hours (all stores by peak txn) ────────────────────
        store_peaks = []
        for store, s_info in store_hours.items():
            hrs = s_info.get("hours", {})
            if not hrs:
                continue
            best_h = max(hrs.items(), key=lambda x: x[1]["txn"])
            sorted_hrs = sorted(hrs.items(), key=lambda x: x[1]["txn"], reverse=True)
            top3_store = " → ".join(fmt_hr(h) for h, _ in sorted_hrs[:3])
            store_peaks.append((store, best_h[0], best_h[1]["txn"], best_h[1]["rev"], top3_store))
        store_peaks.sort(key=lambda x: x[2], reverse=True)

        # Chain-wide unique customers
        chain_total_cust = sum(v["cust"] for v in hour_agg.values())

        lines = [
            "═══ PEAK HOURS CHAIN SUMMARY ════════════════════════════════════════════",
            f"  Chain Peak Hour (bills/txns)   : {fmt_hr(peak_h_txn)}"
            f" — {hour_agg[peak_h_txn]['txn']:,.0f} bills | {hour_agg[peak_h_txn]['cust']:,.0f} unique customers"
            f" | {len(hour_agg[peak_h_txn]['stores'])} stores",
        ]
        if peak_h_rev != peak_h_txn:
            lines.append(
                f"  Chain Peak Hour (revenue)       : {fmt_hr(peak_h_rev)}"
                f" — ₹{hour_agg[peak_h_rev]['rev']:,.0f}"
            )
        if window:
            lines.append(f"  Active Trading Window (≥50% peak): {window}")
        if chain_total_cust > 0:
            lines.append(f"  Total Unique Customers (day)    : {chain_total_cust:,.0f}")
        lines += [
            f"  Top 3 hours by bills     : {top3_txn}",
            f"  Top 3 hours by revenue   : {top3_rev}",
            f"  Stores analysed: {len(store_hours)} | Hours tracked: {fmt_hr(min(all_hours))} to {fmt_hr(max(all_hours))}",
        ]
        if store_peaks:
            lines.append("  Per-Store Peak Hours (all stores, sorted by peak transactions):")
            lines.append(
                "  Store | Zone | Region | Peak Slot | Bills | Unique Cust (Mobile)"
                " | Revenue | UPT | ATV | Top 3 Slots"
            )
            for store, ph, ptxn, prev, top3s in store_peaks:
                s_info = store_hours.get(store, {})
                zone   = s_info.get("zone", "")
                region = s_info.get("region", "")
                hrs    = s_info.get("hours", {})
                peak_cust = hrs.get(ph, {}).get("cust", 0)
                # Compute store-level UPT and ATV across all hours
                store_total_qty  = s_info.get("total_qty", 0)
                store_total_txn  = sum(h.get("txn", 0) for h in hrs.values())
                store_total_rev  = sum(h.get("rev", 0) for h in hrs.values())
                upt_str = f"{store_total_qty / store_total_txn:.2f}" if store_total_txn > 0 else "0.00"
                atv_str = f"₹{store_total_rev / store_total_txn:,.0f}" if store_total_txn > 0 else "₹0"
                lines.append(
                    f"    {store} | {zone} | {region} | {fmt_hr(ph)}"
                    f" | {ptxn:.0f} bills | {peak_cust:.0f}"
                    f" | ₹{prev:,.0f} | {upt_str} | {atv_str} | {top3s}"
                )
        lines.append(
            "  Insight use: staffing schedule, floor replenishment, promotion launch windows"
        )
        lines.append("═══════════════════════════════════════════════════════════════════════════")
        return "\n".join(lines) + "\n"

    except Exception:
        return ""
