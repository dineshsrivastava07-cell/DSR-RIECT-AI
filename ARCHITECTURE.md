# DSR|RIECT — Technical Architecture

**Version:** 3.0.0
**Date:** March 2026
**Author:** Dinesh Srivastava
**Status:** Production

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [High-Level Architecture Diagram](#2-high-level-architecture-diagram)
3. [Layer-by-Layer Breakdown](#3-layer-by-layer-breakdown)
4. [Data Flow — End to End](#4-data-flow--end-to-end)
5. [Pipeline Stages (Orchestrator)](#5-pipeline-stages-orchestrator)
6. [KPI Engine Architecture](#6-kpi-engine-architecture)
7. [Extended KPI Engine (13 KPIs)](#7-extended-kpi-engine-13-kpis)
8. [FY Date Intelligence Engine](#8-fy-date-intelligence-engine)
9. [Alert Engine Architecture](#9-alert-engine-architecture)
10. [LLM Router Architecture](#10-llm-router-architecture)
11. [SQL Generation Engine](#11-sql-generation-engine)
12. [Prompt Builder Architecture](#12-prompt-builder-architecture)
13. [Supplementary Query System](#13-supplementary-query-system)
14. [Product Alignment Engine](#14-product-alignment-engine)
15. [Frontend Architecture](#15-frontend-architecture)
16. [Database Schema](#16-database-schema)
17. [ClickHouse Data Model](#17-clickhouse-data-model)
18. [Active Store Rule](#18-active-store-rule)
19. [Anomaly Detection Design](#19-anomaly-detection-design)
20. [Security Design](#20-security-design)
21. [Performance Characteristics](#21-performance-characteristics)
22. [Deployment Architecture](#22-deployment-architecture)
23. [Key Design Decisions](#23-key-design-decisions)

---

## 1. System Overview

DSR|RIECT is a **single-server, on-premises retail intelligence platform** comprising:

- A **FastAPI backend** (port 8001) serving REST APIs + WebSocket real-time chat
- A **ClickHouse connection** to a remote retail data warehouse (read-only)
- A **SQLite database** for local config, alerts, session history
- A **multi-LLM routing layer** supporting 5 provider families
- A **deterministic KPI engine** (no ML inference — pure Python/pandas)
- A **single-page JavaScript frontend** served statically by FastAPI

The system is designed for **air-gapped or on-premises deployment** with no mandatory cloud dependency (Ollama supports fully offline LLM).

---

## 2. High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DSR|RIECT PLATFORM                                   │
│                                                                               │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                        FRONTEND (Browser)                              │   │
│  │  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────┐  │   │
│  │  │ KPI Tiles   │  │ Exception    │  │ AI Chatbot   │  │ Settings │  │   │
│  │  │ (Live Data) │  │ Inbox P1-P3  │  │ (WebSocket)  │  │ Panel    │  │   │
│  │  └─────────────┘  └──────────────┘  └──────────────┘  └──────────┘  │   │
│  └────────────────────────────┬─────────────────────────────────────────┘   │
│                               │ HTTP / WebSocket                              │
│  ┌────────────────────────────▼─────────────────────────────────────────┐   │
│  │                    FASTAPI BACKEND (Port 8001)                         │   │
│  │                                                                         │   │
│  │  ┌──────────────┐  ┌─────────────────────────────────────────────┐   │   │
│  │  │  REST APIs   │  │           PIPELINE ORCHESTRATOR              │   │   │
│  │  │ /api/kpi/*   │  │  Intent → Normalise → Schema → SQL → KPI → │   │   │
│  │  │ /api/alerts/*│  │  Supplementary → Anomaly → Prompt → Stream  │   │   │
│  │  └──────────────┘  └─────────────────────────────────────────────┘   │   │
│  │                                                                         │   │
│  │  ┌────────────────────────────────────────────────────────────────┐   │   │
│  │  │                    ENGINE LAYER                                  │   │   │
│  │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │   │   │
│  │  │  │ SPSF     │  │ Sell-    │  │ DOI      │  │ Anomaly      │  │   │   │
│  │  │  │ Engine   │  │ Thru Eng │  │ Engine   │  │ Engine       │  │   │   │
│  │  │  └──────────┘  └──────────┘  └──────────┘  └──────────────┘  │   │   │
│  │  └────────────────────────────────────────────────────────────────┘   │   │
│  │                                                                         │   │
│  │  ┌────────────────────────────────────────────────────────────────┐   │   │
│  │  │                    LLM ROUTER                                    │   │   │
│  │  │  Claude Sonnet → OpenAI GPT-4o → Gemini → Qwen → Ollama        │   │   │
│  │  └────────────────────────────────────────────────────────────────┘   │   │
│  └────────────┬────────────────────────────────────────┬─────────────────┘   │
│               │                                         │                      │
│  ┌────────────▼──────────┐              ┌──────────────▼──────────────────┐  │
│  │   SQLite (riect.db)   │              │   ClickHouse (Remote)            │  │
│  │  - Sessions/History   │              │   chn1.vmart-tools.com:8443      │  │
│  │  - Alerts             │              │   vmart_sales.*                  │  │
│  │  - Settings/Keys      │              │   vmart_product.*                │  │
│  │  - Store SQFT         │              │   (Read-Only)                    │  │
│  └───────────────────────┘              └─────────────────────────────────┘  │
│                                                                               │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                      LLM Providers (External)                          │   │
│  │  Anthropic API  │  OpenAI API  │  Google API  │  Qwen API  │  Ollama  │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Layer-by-Layer Breakdown

### Layer 1 — Presentation (Frontend)
**Files:** `app/frontend/index.html`, `app.js`, `styles.css`

Single-page application with four panels:
- **Left Pane**: Session history, chat input
- **KPI Bar**: Live SPSF, Sell-Through, DOI, UPT tiles with P1/P2/P3 colour coding
- **Exception Inbox**: Live alert list (P1 🔴 → P2 🟠 → P3 🟡)
- **Chat Area**: Streaming markdown responses with scrollable tables

### Layer 2 — API Gateway (FastAPI)
**File:** `app/backend/main.py`

- REST endpoints for settings, schema, alerts, KPIs
- WebSocket `/ws/chat` — bidirectional streaming chat
- Static file serving (frontend)
- OAuth integration hooks
- CORS middleware (open for on-prem deployment)

### Layer 3 — Pipeline Orchestrator
**File:** `app/backend/pipeline/orchestrator.py`

The central brain — 11-stage pipeline described in Section 5.

### Layer 4 — Engine Layer
**Directory:** `app/backend/riect/`

Deterministic Python engines — no LLM inference for KPI computation:
- `kpi_engine/` — SPSF, Sell-Through, DOI, MBQ, Anomaly + Extended KPI Engine (13 KPIs)
- `alert_engine/` — Priority classification, alert generation, action playbooks
- `product_engine/` — Product alignment cache layer (ClickHouse → SQLite fast lookups)

### Layer 4b — FY Date Intelligence
**File:** `app/backend/pipeline/date_engine.py`

Indian Financial Year (Apr 1 – Mar 31) date intelligence engine providing:
- YTD, MTD, WTD, QTD, LTL (Like-for-Like) period resolution
- FY week numbering (Week 1 = first Monday on or after Apr 1)
- Days elapsed in FY, prior FY same-period alignment

### Layer 5 — Data Layer
- **ClickHouse** (remote, read-only): real-time retail transactional + inventory data
- **SQLite** (local): config, alerts, sessions, store master

### Layer 6 — LLM Layer
**Directory:** `app/backend/llm/`

Multi-provider LLM routing with fallback. Used for SQL generation and analytical response.

---

## 4. Data Flow — End to End

### Chat Request Flow

```
1. User types query in browser
        │
        ▼
2. WebSocket message → FastAPI /ws/chat
        │
        ▼
3. PipelineOrchestrator.decide(query)
   → Intent classification
   → Route assignment (KPI_ANALYSIS / DATA_QUERY / PEAK_HOURS / ...)
   → Normalisation (date extraction, typo fix)
        │
        ▼
4. Schema load (ClickHouse → schema_inspector, 1hr cache)
        │
        ▼
5. Data freshness check
   → "SELECT max(toDate(BILLDATE)) WHERE COUNT(DISTINCT BILLNO) >= 10000"
   → latest_sales_date injected into context
        │
        ▼
6. SQL Generation (LLM)
   → System prompt: ClickHouse rules, table schema, column ownership
   → User prompt: schema context, join hints, route-specific SQL hints
   → SQL validated (no DROP/DELETE/ALTER), auto-retried on ClickHouse error
        │
        ▼
7. SQL Execution (ClickHouse → query_runner)
   → Returns {data: [...], columns: [...], row_count: N}
        │
        ▼
8. Alias normalisation (canonical column names)
   → "netamt" → "net_sales_amount", "qty_sold" → "total_qty", etc.
        │
        ▼
9. SQFT enrichment
   → Join floor_sqft from SQLite store master by STORE_ID
   → Compute spsf = net_sales_amount / floor_sqft
        │
        ▼
10. Supplementary queries (auto, 4 queries for comprehensive analysis)
    → Department breakdown (MTD)
    → Article breakdown — top 25 (MTD)
    → Article breakdown — bottom 25 slowest movers (MTD)
    → Peak hours per store (latest day)
    → Top 7 highest MRP articles (MTD)
        │
        ▼
11. KPI engines
    → spsf_engine, sell_thru_engine, doi_engine, anomaly_engine
    → Returns {SPSF, SELL_THRU, DOI, anomalies, p1/p2/p3 counts}
        │
        ▼
12. Alert generation
    → KPI breaches → AlertRecord objects → enrich with action playbooks → save to SQLite
        │
        ▼
13. Prompt build
    → ANALYTICAL_SYSTEM_ADDENDUM (8-section format rules)
    → Inject: chain totals, KPI summaries, cross-KPI per-store table
    → Inject: anomaly detection output
    → Inject: supplementary data (dept, articles, peak hours, MRP)
        │
        ▼
14. LLM stream
    → System prompt + user prompt → chosen LLM provider
    → Token-by-token streaming → WebSocket "token" events → browser renders live
        │
        ▼
15. Response formatting
    → Structured blocks: narrative (markdown), table (raw data), sql, alerts, kpis
    → Saved to session history (SQLite)
```

### Live KPI Dashboard Flow

```
Browser loads / user clicks refresh
    │
    ▼
GET /api/kpi/live
    │
    ▼
1. Find latest complete date (≥10k bills)
2. Chain sales aggregate for that day (ACTIVE stores only)
3. Per-store SPSF computation (join floor_sqft from SQLite)
4. Item-level sell-through + DOI (pre-aggregated, no row multiplication)
5. Derived: ATV, UPT, Discount Rate
    │
    ▼
Return JSON → KPI tile rendering in browser
```

---

## 5. Pipeline Stages (Orchestrator)

**File:** `pipeline/orchestrator.py` — class `PipelineOrchestrator`

| Stage | Condition | What Happens |
|---|---|---|
| `context_build` | Always | Build base context dict (query, session, schema hints) |
| `schema_load` | SQL routes | Load ClickHouse table/column schema, cache 1hr |
| `data_freshness` | SQL routes | Detect latest complete sales date from ClickHouse |
| `sql_generate` | SQL routes | LLM generates ClickHouse SQL with route-specific hints |
| `sql_execute` | SQL ready | Run query, collect {data, columns, row_count} |
| `sql_retry` | On error | Feed error + failed SQL back to LLM for self-correction |
| `alias_normalise` | Data present | Rename LLM-deviated aliases to pipeline canonical names |
| `sqft_enrich` | Data present | Join floor_sqft, compute `spsf` column |
| `supplementary_queries` | KPI/DATA routes | Run 4-5 additional queries (dept/article/peak/MRP) |
| `kpi_analyse` | KPI routes | Run SPSF, ST%, DOI, MBQ, Anomaly engines |
| `alert_generate` | KPI routes | Generate P1/P2/P3 alerts, save to SQLite |
| `prompt_build` | Always | Assemble final (system, user) prompt pair |
| `llm_stream` | LLM routes | Stream response token-by-token → WebSocket |

### Route Taxonomy

```python
Route.GREETING        # Hi, hello, what can you do
Route.GENERAL_CHAT    # Retail Q&A, no live data
Route.ALERT_REVIEW    # Show alerts / P1 inbox
Route.SCHEMA_BROWSE   # What tables exist
Route.DATA_QUERY      # Show data (single SQL)
Route.KPI_ANALYSIS    # Full KPI + engines + anomaly
Route.TREND_ANALYSIS  # Multi-period SQL + trend
Route.VENDOR_ANALYSIS # PO/GR + supply chain
Route.PEAK_HOURS      # Hourly traffic/sales
```

---

## 6. KPI Engine Architecture

**Directory:** `riect/kpi_engine/`

### SPSF Engine

```
Input DataFrame: [store_id, store_name, net_sales_amount, floor_sqft]

1. Filter: floor_sqft >= 300 (MIN_SQFT_FOR_SPSF)
2. Filter: net_sales_amount > 0
3. Project MTD → Monthly:
   spsf = (net_sales_amount / floor_sqft) × (days_in_month / days_elapsed)
4. Classify:
   P1 if spsf < 500
   P2 if spsf < 750
   P3 if spsf < 1000
   OK if spsf >= 1000

Output:
  - Per-store: {store_id, store_name, spsf, priority, gap_to_target}
  - Summary: {avg_spsf, p1_count, p2_count, p3_count, on_target}
  - Breach rows: P1 + P2 stores sorted by spsf ASC (worst first)
```

### Sell-Through Engine

```
Formula (CRITICAL — prevents row multiplication):

CHAIN LEVEL:
  Inner: GROUP BY i.ICODE
    icode_soh = SUM(i.SOH)
    icode_qty = COALESCE(SUM(p.QTY), 0)
    st_pct    = icode_qty / (icode_qty + icode_soh) × 100

  Outer: avgIf(st_pct, st_pct > 0)    → chain avg ~44-46%
         DOI = sumIf(soh, qty>0) / sumIf(qty, qty>0)

STORE LEVEL:
  inventory subquery: GROUP BY STORE_CODE → store_soh
  sales subquery:     GROUP BY STORE_ID   → store_qty
  JOIN on STORE_CODE = toString(STORE_ID)
  sell_thru = store_qty / (store_qty + store_soh) × 100

Rules:
  - NEVER filter WHERE SOH > 0 (sold-out items = 100% ST% — correct)
  - NEVER raw JOIN inventory × sales on ICODE (row multiplication)
  - Both subqueries must be pre-aggregated before joining
```

### DOI Engine

```
MTD formula (per store):
  DOI = store_soh / (store_mtd_qty / days_elapsed)

Single-day formula (chain level):
  DOI = sumIf(icode_soh, icode_qty > 0)
        / sumIf(icode_qty, icode_qty > 0)

Classification:
  P1 if doi > 90 days
  P2 if doi > 60 days
  P3 if doi > 30 days
  OK if doi ≤ 30 days
```

### Anomaly Engine

```python
# Z-score with directional masking per KPI
Z_THRESHOLD = 2.0

KPI_ANOMALY_COLUMNS = {
    "spsf":           ("SPSF",     "Sales Per Sq Ft",       bad_direction="low"),
    "sell_thru_pct":  ("SELL_THRU","Sell-Through %",         bad_direction="low"),
    "upt":            ("UPT",      "Units Per Transaction",  bad_direction="low"),
    "doi":            ("DOI",      "Days of Inventory",      bad_direction="high"),
    "net_sales":      ("SALES",    "Net Sales",              bad_direction="low"),
    "soh":            ("SOH",      "Stock on Hand",          bad_direction="both"),
}

# Directional masking prevents top performers appearing as anomalies
if bad_direction == "low":
    anomaly_mask = z_scores <= -Z_THRESHOLD   # only underperformers
elif bad_direction == "high":
    anomaly_mask = z_scores >= Z_THRESHOLD    # only overstock
else:
    anomaly_mask = z_scores.abs() >= Z_THRESHOLD
```

---

## 7. Extended KPI Engine (13 KPIs)

**File:** `riect/kpi_engine/extended_kpi_engine.py`
**Registry:** `pipeline/kpi_alignment.py`

### KPI Alignment Registry (`kpi_alignment.py`)

```python
KPI_REGISTRY = {
    # Core KPIs
    "SPSF":               {"label": "Sales Per Sq Ft",        "category": "productivity"},
    "SELL_THRU":          {"label": "Sell-Through %",          "category": "inventory"},
    "DOI":                {"label": "Days of Inventory",       "category": "inventory"},
    "MBQ":                {"label": "Min Buy Qty Compliance",  "category": "inventory"},
    # Extended KPIs
    "ATV":                {"label": "Avg Transaction Value",   "category": "transaction"},
    "UPT":                {"label": "Units Per Transaction",   "category": "transaction"},
    "DISCOUNT_RATE":      {"label": "Discount Rate %",         "category": "margin"},
    "NON_PROMO_DISC":     {"label": "Non-Promo Discount %",    "category": "margin"},
    "GROSS_MARGIN":       {"label": "Gross Margin %",          "category": "margin"},
    "MOBILE_PENETRATION": {"label": "Mobile Customer %",       "category": "customer"},
    "BILL_INTEGRITY":     {"label": "Bill Integrity %",        "category": "operations"},
    "GIT_COVERAGE":       {"label": "GIT Coverage (days)",     "category": "supply_chain"},
    "AOP_VS_ACTUAL":      {"label": "AOP vs Actual %",         "category": "planning"},
}
```

`detect_available_kpis(df)` — auto-detects which KPIs can be computed from available columns.
`get_available_categories(df)` — returns groupings for prompt section building.

### Extended Engine Groups

| Engine Group | KPIs Computed | Formula |
|---|---|---|
| **ATV** | Avg Transaction Value | `net_sales_amount / bill_count` |
| **UPT** | Units Per Transaction | `total_qty / bill_count` |
| **Discount Rate** | Discount Rate % | `discount_amount / gross_amount × 100` |
| **Non-Promo Discount** | Non-Promo Disc % | `discount_amount / gross_amount × 100` (non-promo bills only) |
| **Gross Margin** | Gross Margin % | `(net_sales - cost_of_goods) / net_sales × 100` |
| **Mobile Penetration** | Mobile Customer % | `distinct_mobile / total_bills × 100` |
| **Bill Integrity** | Bill Integrity % | `valid_bills / total_bills × 100` |
| **SOH Health** | SOH Health Score | `available_soh / total_soh × 100` |
| **GIT Coverage** | GIT Coverage (days) | `git_qty / avg_daily_sales` |
| **MBQ Shortfall** | MBQ Shortfall Amount | `(mbq_target - current_soh) × cost_price` |
| **AOP vs Actual** | AOP vs Actual % | `actual_sales / aop_target × 100` |

### KPI Controller Integration

`kpi_controller.py` exposes:
```python
run_all(query_result)     → unified result with kpi_availability + available_categories
run_extended(df, context) → extended KPI metrics dict
```

Output includes `kpi_availability` (which KPIs are computable from data) and `available_categories` (for prompt section selection), allowing the prompt builder to inject only relevant KPI sections.

### Extended Anomaly Detection

`anomaly_engine.py` extended with:

```python
KPI_ANOMALY_COLUMNS = {
    ...existing...
    "atv":              ("ATV",              "Avg Transaction Value",  "low"),
    "discount_rate":    ("DISCOUNT_RATE",    "Discount Rate %",        "high"),
    "mobile_pct":       ("MOBILE_PENET",     "Mobile Penetration %",   "low"),
    "bill_integrity":   ("BILL_INTEGRITY",   "Bill Integrity %",       "low"),
    "gross_margin_pct": ("GROSS_MARGIN",     "Gross Margin %",         "low"),
}
```

---

## 8. FY Date Intelligence Engine

**File:** `pipeline/date_engine.py`

Indian Financial Year: **April 1 → March 31**

### Period Definitions

| Period | Resolution |
|---|---|
| `YTD` | Apr 1 of current FY → `latest_sales_date` |
| `MTD` | 1st of current month → `latest_sales_date` |
| `WTD` | Monday of current FY week → `latest_sales_date` |
| `QTD` | Start of current FY quarter → `latest_sales_date` |
| `LTL` | Like-for-Like: same date range in prior FY (FY-1) |
| `FY_FULL` | Apr 1 → Mar 31 of current or prior FY |

### FY Week Numbering

- **Week 1** = first Monday on or after April 1
- Aligns with retail industry standard week reporting
- `days_elapsed_fy` = number of days from Apr 1 to `latest_sales_date`

### LTL (Like-for-Like) Pattern

Used in ClickHouse SQL for same-period YoY comparison:
```sql
-- Current period
sumIf(NETAMT, toDate(BILLDATE) BETWEEN '2025-04-01' AND '2026-02-28') AS current_ytd,
-- Prior FY same period
sumIf(NETAMT, toDate(BILLDATE) BETWEEN '2024-04-01' AND '2025-02-28') AS prior_ytd,
-- Growth %
round((current_ytd - prior_ytd) / prior_ytd * 100, 1) AS ytd_growth_pct
```

Single-query dual-range pattern — no subqueries, no row duplication.

---

## 9. Alert Engine Architecture

**Directory:** `riect/alert_engine/`

### Flow

```
live_scanner.py (startup + POST /api/alerts/scan)
    │
    ├── Query ClickHouse (SPSF + ST% + DOI per store)
    │
    ├── Run KPI engines
    │
    ├── get_breach_rows() → pandas DataFrame of breaching stores
    │
    ▼
alert_generator.py → generate_alerts(kpi_results)
    │
    ├── For each breach row: create AlertRecord
    │   { kpi_type, dimension_value, metric_value, chain_avg,
    │     gap_to_target, priority, session_id, exception_text }
    │
    ▼
action_recommender.py → enrich_alerts_with_actions(alerts)
    │
    ├── Map {kpi_type, priority} → action playbook
    │   { action, owner, timeline, impact }
    │
    ▼
alert_store.py → save_alerts(alerts)
    │
    └── SQLite: riect_alerts table
```

### Alert Record Structure

```python
@dataclass
class AlertRecord:
    id:               str       # UUID
    session_id:       str       # scan session identifier
    kpi_type:         str       # "SPSF" | "SELL_THRU" | "DOI" | "MBQ"
    dimension:        str       # "store" | "category" | "sku"
    dimension_value:  str       # Store name / category / SKU
    metric_value:     float     # Actual KPI value
    chain_avg:        float     # Chain average for context
    gap_to_target:    float     # Distance from target
    priority:         str       # "P1" | "P2" | "P3"
    exception_text:   str       # Human-readable exception description
    action:           str       # Recommended action
    owner:            str       # Who should act
    timeline:         str       # By when
    created_at:       datetime
```

### Priority Classification Rules

```python
# priority_engine.py
def classify_priority(kpi_type: str, value: float) -> str:
    if kpi_type == "SPSF":
        if value < 500:  return "P1"
        if value < 750:  return "P2"
        if value < 1000: return "P3"
        return "OK"

    if kpi_type == "SELL_THRU":   # value is fraction 0-1
        if value < 0.60: return "P1"
        if value < 0.80: return "P2"
        if value < 0.95: return "P3"
        return "OK"

    if kpi_type == "DOI":
        if value > 90: return "P1"
        if value > 60: return "P2"
        if value > 30: return "P3"
        return "OK"
```

---

## 10. LLM Router Architecture

**File:** `llm/llm_router.py`

```
User Query
    │
    ▼
get_router(model_preference)
    │
    ├── If Anthropic key available → CloudClient(provider="anthropic")
    ├── If OpenAI key → CloudClient(provider="openai")
    ├── If Gemini key → CloudClient(provider="google")
    ├── If Qwen → QwenClient()
    └── If Ollama running → OllamaClient()

Each client implements:
    .generate(system_prompt, user_prompt, max_tokens, temperature) → str
    .stream(system_prompt, user_prompt, max_tokens, temperature) → AsyncIterator[str]
```

### Token Budget

| Use Case | Temperature | Max Tokens |
|---|---|---|
| SQL generation | 0.1 (deterministic) | 1,500 |
| Analytical response | 0.3 (factual) | 8,000 |

---

## 11. SQL Generation Engine

**File:** `pipeline/sql_generator.py`

### System Prompt Structure

The SQL system prompt enforces strict rules through a layered structure:

```
ABSOLUTE RULE 0 — Column ownership (SHRTNAME not in stores)
CRITICAL RULES — 8 fundamental ClickHouse SQL rules
COLUMN OWNERSHIP — table-by-table column reference
DATE RULES — latest_sales_date, target_date, MTD/WTD/QTD/YTD patterns
SPSF DATE RULE — always MTD, never single-day for SPSF
INVENTORY RULES — pre-aggregation, join patterns, sell-through formula
PERMANENTLY REMOVED — tables that do not exist
KEY COLUMN ALIASES — STORE_ID = CODE = STORE_CODE, ICODE = ARTICLECODE
LABEL SELECTION RULES 1-7 — what to include per query type
MANDATORY OUTPUT ALIASES — exact column names required by pipeline
MANDATORY GROUPING RULES — store-level vs category vs article
```

### Post-Processing (`_post_process_sql`)

Auto-fixes LLM errors before execution:
- `stores.SHRTNAME` → `pos.SHRTNAME` (stores table has no SHRTNAME)
- Fixes GROUP BY references after alias correction
- Strips markdown code fences from LLM output

### SQL Validation (`validate_sql_basic`)

```python
DANGEROUS = ["DROP", "DELETE", "TRUNCATE", "ALTER",
             "INSERT", "UPDATE", "CREATE", "GRANT", "REVOKE"]
# Rejects any non-SELECT or dangerous-keyword SQL
```

---

## 12. Prompt Builder Architecture

**File:** `pipeline/prompt_builder.py`

### Response Format Protocol (8 Sections)

```
SECTION 1 — EXECUTIVE SUMMARY         (2-3 sentences, numbers first)
  1A — Productivity KPIs   (SPSF, ATV, UPT)
  1B — Inventory KPIs      (ST%, DOI, SOH Health, GIT Coverage)
  1C — Margin KPIs         (Gross Margin%, Discount Rate%, Non-Promo Disc%)
  1D — Customer KPIs       (Mobile Penetration%)
  1E — Operations KPIs     (Bill Integrity%)
  1F — MBQ Compliance      (MBQ Shortfall Amount)
  1G — Supply Chain KPIs   (GIT Coverage)
  1H — AOP vs Actual       (Planning deviation%)
SECTION 2 — KPI SCORECARD TABLE       (Chain avg vs target vs P1/P2/P3)
SECTION 3 — STORE PERFORMANCE         (Top 10 + Bottom 10 with insights)
SECTION 4 — DEPT & ARTICLE ANALYSIS   (Top/Bottom 10 departments + articles)
SECTION 5 — TOP 7 HIGHEST MRP         (Premium product performance)
SECTION 6 — ANOMALIES                 (Z-score flagged, IST/Markdown guidance)
SECTION 7 — PEAK HOURS                (All stores, bill+mobile customer counts)
SECTION 8 — PRIORITY ACTIONS          (WHO + WHAT + HOW MUCH + WHEN)
```

### KPI Availability Map

`_build_kpi_sections()` helper inspects available KPIs from `kpi_availability` and injects a `KPI AVAILABILITY MAP` block into the user prompt — so the LLM only generates sections for KPIs actually present in the data:

```
═══ KPI AVAILABILITY MAP ══════════════════════════════
  PRODUCTIVITY:  SPSF ✓ | ATV ✓ | UPT ✓
  INVENTORY:     SELL_THRU ✓ | DOI ✓ | MBQ ✗ | GIT ✗
  MARGIN:        GROSS_MARGIN ✗ | DISCOUNT_RATE ✓
  CUSTOMER:      MOBILE_PENETRATION ✓
  OPERATIONS:    BILL_INTEGRITY ✗
  PLANNING:      AOP_VS_ACTUAL ✗
═══════════════════════════════════════════════════════
```

### Injected Context Blocks

```
═══ CHAIN TOTALS ══════════════════════
  Chain Net Sales Amount : ₹X
  Chain Total Qty        : Y
  Chain Total Bills      : Z
  ← LLM must use ONLY these for chain totals

═══ SPSF CHAIN SUMMARY ════════════════
  Chain avg MTD SPSF : ₹X/sqft
  P1: N stores | P2: N | P3: N | On Target: N
  Top 3 / Bottom 3 stores by SPSF

═══ DOI CHAIN SUMMARY ═════════════════
═══ SELL-THROUGH CHAIN SUMMARY ════════
═══ UPT CHAIN SUMMARY ═════════════════

═══ PER-STORE CROSS-KPI VIEW ══════════
  Store | SPSF | ATV | Sell-Through% | DOI | UPT
  (ATV computed inline: net_sales_amount / bill_count)

═══ ANOMALY DETECTION OUTPUT ══════════
  [from anomaly_engine — directional z-score]

═══ SUPPLEMENTARY DATA ════════════════
  [dept, articles, peak hours, top MRP blocks]
```

### ATV Inline Computation

The cross-KPI table computes ATV without requiring it as a pre-built column:

```python
if has_atv:  # sales_col AND bill_col both present
    net   = float(row.get(sales_col, 0) or 0)
    bills = float(row.get(bill_col,  0) or 0)
    atv_v = round(net / bills, 0) if bills > 0 else None
    parts.append(f"₹{int(atv_v):,}" if atv_v else "-")
```

---

## 13. Supplementary Query System

**Method:** `PipelineOrchestrator._run_supplementary_queries(context)`

Triggered automatically after main query when:
- Route is `KPI_ANALYSIS`, `DATA_QUERY`, or `TREND_ANALYSIS`
- Main query returned ≥ 3 rows
- `latest_sales_date` is known

### Queries Executed

| Query Key | SQL Pattern | Rows | Purpose |
|---|---|---|---|
| `dept` | GROUP BY DIVISION, SECTION, DEPARTMENT | 25 | Section 4 Dept Top 10 |
| `articles` | GROUP BY ICODE … ORDER BY net_sales DESC | 25 | Section 4 Article Top 10 |
| `articles_bottom` | GROUP BY ICODE … ORDER BY net_sales ASC | 25 | Section 4 Article Bottom 10 |
| `peak_hours` | GROUP BY STORE_ID, hour | 500 | Section 7 Peak Hours |
| `top_mrp` | JOIN vitem_data ORDER BY unit_mrp DESC | 7 | Section 5 Top MRP |

All queries apply the active store filter. Each runs in a try/except — one failure does not abort the others.

The results are formatted by `_format_supplementary_data()` and injected as a clearly labelled `SUPPLEMENTARY DATA (use for Sections 4, 5, 7)` block in the LLM user prompt.

---

## 15. Frontend Architecture

**Directory:** `app/frontend/`

### Single-Page Architecture

```
index.html
├── Left Pane
│   ├── Session history list
│   └── New chat button
│
├── Centre/Main
│   ├── KPI Bar (live tiles via GET /api/kpi/live)
│   │   ├── SPSF tile (daily + monthly projected)
│   │   ├── Sell-Through tile
│   │   ├── DOI tile (Days, P1/P2/P3 colour)
│   │   └── UPT + ATV + Discount Rate
│   │
│   ├── Chat Container
│   │   ├── Message bubbles (user + assistant)
│   │   ├── Streaming markdown render (marked.js)
│   │   ├── Scrollable table wrappers (narrative-table-wrap)
│   │   └── Response tabs: Narrative | Data | SQL | Alerts
│   │
│   └── Input bar + Send button
│
└── Right Pane
    ├── Exception Inbox (P1 → P2 → P3 alerts)
    └── Settings panel (ClickHouse, LLM, SQFT)
```

### Scrollable Table Design

Every markdown table rendered by the LLM is automatically wrapped in a scrollable container:

```javascript
// renderMarkdown() post-processes marked.parse() output
html = html.replace(/<table>/g, '<div class="narrative-table-wrap"><table>');
html = html.replace(/<\/table>/g, '</table></div>');
```

```css
.narrative-table-wrap {
    overflow-x: auto;
    overflow-y: auto;
    max-height: 440px;       /* vertical scroll for tall tables */
    border: 1px solid var(--border);
    border-radius: var(--r-sm);
}
.response-narrative table th {
    position: sticky;        /* header stays visible on scroll */
    top: 0;
    z-index: 2;
}
```

### WebSocket State Machine

```javascript
// Incoming event types → state transitions
"route"          → show pipeline route badge
"stage"          → show stage spinner
"sql_generated"  → show SQL in tab
"data_ready"     → show row count
"kpi_done"       → update P1/P2/P3 counts
"token"          → append to rawNarrative → re-render markdown
"alerts_ready"   → update exception inbox
```

---

## 14. Product Alignment Engine

**Files:** `riect/product_engine/product_alignment.py`, `riect/api/product_api.py`

Caches product master data from ClickHouse into SQLite for fast, low-latency lookups without hitting ClickHouse on every request.

### Cache Layer Design

```
ClickHouse (vmart_product.vitem_data)
        │
        ▼  [scheduled or on-demand sync]
product_alignment (SQLite table)
        │
        ▼
GET /api/products/...  →  sub-millisecond response
```

### SQLite Table: `product_alignment`

```sql
CREATE TABLE product_alignment (
    icode            TEXT PRIMARY KEY,
    article_code     TEXT,
    article_name     TEXT,
    division         TEXT,
    section          TEXT,
    department       TEXT,
    option_code      TEXT,
    cost_price       REAL,
    mrp              REAL,
    item_description TEXT,
    supplier_name    TEXT,
    style_or_pattern TEXT,
    size             TEXT,
    color            TEXT,
    cached_at        TEXT
);
CREATE INDEX idx_pa_division   ON product_alignment(division);
CREATE INDEX idx_pa_section    ON product_alignment(section);
CREATE INDEX idx_pa_dept       ON product_alignment(department);
```

### Product API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/products/search?q=...` | Search articles by name/code |
| `GET` | `/api/products/{icode}` | Lookup single article by ICODE |
| `GET` | `/api/products/division/{name}` | List articles by division |
| `POST` | `/api/products/sync` | Trigger cache refresh from ClickHouse |

---

## 16. Database Schema

**File:** `app/backend/db.py` (SQLite — `riect.db`)

### Tables

```sql
-- Application settings (ClickHouse config, LLM keys)
CREATE TABLE IF NOT EXISTS settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT
);

-- Conversation sessions
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    created_at TEXT,
    title      TEXT,
    role       TEXT DEFAULT 'HQ'
);

-- Message history per session
CREATE TABLE IF NOT EXISTS messages (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT,
    role       TEXT,     -- 'user' | 'assistant'
    content    TEXT,
    created_at TEXT,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

-- ClickHouse schema discovery cache (1hr TTL)
CREATE TABLE IF NOT EXISTS schema_cache (
    schema_name  TEXT,
    table_name   TEXT,
    columns_json TEXT,
    cached_at    TEXT,
    PRIMARY KEY (schema_name, table_name)
);

-- Store floor sqft master (755 stores loaded from CSV)
CREATE TABLE IF NOT EXISTS store_sqft (
    store_id   INTEGER PRIMARY KEY,
    store_name TEXT,
    shrtname   TEXT,
    sitetype   TEXT,
    floor_sqft INTEGER DEFAULT 0,
    city_name  TEXT,
    updated_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_store_sqft_shrtname ON store_sqft(shrtname);

-- RIECT Plan: KPI target overrides (config.py defaults unless overridden here)
CREATE TABLE IF NOT EXISTS riect_plan (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    kpi_type        TEXT NOT NULL,
    dimension       TEXT DEFAULT 'global',
    dimension_value TEXT DEFAULT '',
    p1_threshold    REAL,
    p2_threshold    REAL,
    p3_threshold    REAL,
    target          REAL,
    period          TEXT DEFAULT '',
    notes           TEXT DEFAULT '',
    updated_at      TEXT,
    UNIQUE(kpi_type, dimension, dimension_value)
);

-- Product master cache (ClickHouse → SQLite for fast lookups)
CREATE TABLE IF NOT EXISTS product_alignment (
    icode            TEXT PRIMARY KEY,
    article_code     TEXT,
    article_name     TEXT,
    division         TEXT,
    section          TEXT,
    department       TEXT,
    option_code      TEXT,
    cost_price       REAL,
    mrp              REAL,
    item_description TEXT,
    supplier_name    TEXT,
    style_or_pattern TEXT,
    size             TEXT,
    color            TEXT,
    cached_at        TEXT
);
CREATE INDEX IF NOT EXISTS idx_pa_division ON product_alignment(division);
CREATE INDEX IF NOT EXISTS idx_pa_section  ON product_alignment(section);
CREATE INDEX IF NOT EXISTS idx_pa_dept     ON product_alignment(department);

-- Alert/exception store
CREATE TABLE IF NOT EXISTS riect_alerts (
    alert_id           TEXT PRIMARY KEY,
    created_at         TEXT NOT NULL,
    session_id         TEXT,
    priority           TEXT NOT NULL,    -- P1 | P2 | P3 | P4
    kpi_type           TEXT NOT NULL,    -- SPSF | SELL_THRU | DOI | MBQ
    signal_type        TEXT NOT NULL,    -- SPSF_BREACH | SELL_THRU_BREACH | ...
    dimension          TEXT NOT NULL,    -- store | category | sku
    dimension_value    TEXT NOT NULL,    -- Store name / category / SKU
    kpi_value          REAL,
    threshold          REAL,
    gap                REAL,
    status             TEXT,             -- OPEN | RESOLVED
    exception_text     TEXT,
    recommended_action TEXT,
    action_owner       TEXT,
    response_timeline  TEXT,
    expected_impact    TEXT,
    resolved           INTEGER DEFAULT 0,
    resolved_at        TEXT
);
```

---

## 17. ClickHouse Data Model

**Remote cluster:** `chn1.vmart-tools.com:8443` (HTTPS, read-only)

### Schema: `vmart_sales`

| Table | Key Columns | Purpose |
|---|---|---|
| `pos_transactional_data` | STORE_ID, SHRTNAME, ZONE, REGION, BILLDATE, BILLNO, ICODE, ARTICLENAME, DIVISION, SECTION, DEPARTMENT, NETAMT, QTY, GROSSAMT, DISCOUNTAMT, MRPAMT, STYLE_OR_PATTERN, SIZE, COLOR, CUSTOMER_MOBILE | Primary POS sales — all store transactions |
| `omni_transactional_data` | STORE_ID, ORDERID, BILLDATE, ICODE, NETAMT, QTY, … | Online/omni-channel sales |
| `stores` | CODE, STORE_NAME, REGION, ZONE, AREA_TIER, CLOSING_DATE | Store master — used for active store filter |

### Schema: `vmart_product`

| Table | Key Columns | Purpose |
|---|---|---|
| `inventory_current` | ICODE, STORE_CODE, SOH, UPDATED_AT, _VERSION | Live stock-on-hand (no date filter needed) |
| `vitem_data` | ICODE, ARTICLECODE, ARTICLENAME, MRP, RATE, GRPNAME | Item master — MRP, cost, product hierarchy |

### Key Join Patterns

```sql
-- Sales → Inventory (store level)
toString(p.STORE_ID) = inv.STORE_CODE

-- Sales → Item master
p.ICODE = v.ICODE

-- Active store filter (applied to ALL queries)
STORE_ID NOT IN (SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL)
```

---

## 18. Active Store Rule

**Rule**: All queries exclude stores where `CLOSING_DATE IS NOT NULL` in `vmart_sales.stores`.

This rule is applied at three levels:

| Context | Filter Applied |
|---|---|
| Sales table | `STORE_ID NOT IN (SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL)` |
| Inventory table | `STORE_CODE NOT IN (SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL)` |
| LLM-generated SQL | Injected as a CRITICAL RULE in the SQL system prompt |
| Live scanner | Constants `ACTIVE_STORE_FILTER` + `ACTIVE_STORE_FILTER_INV` in `live_scanner.py` |
| KPI API | Same constants in `kpi_api.py` |

---

## 19. Anomaly Detection Design

**File:** `riect/kpi_engine/anomaly_engine.py`

### Algorithm

```
1. For each KPI column in the query result:
   a. Extract numeric values (skip None/NaN)
   b. Compute z-score = (value - mean) / std
   c. Apply directional mask:
      - bad_direction="low"  → flag only z ≤ -2.0  (underperformers)
      - bad_direction="high" → flag only z ≥ +2.0  (overstock)
      - bad_direction="both" → flag |z| ≥ 2.0

2. P-level:
   |z| ≥ 3.0 → P1 (critical anomaly)
   |z| ≥ 2.0 → P2 (high anomaly)

3. For each flagged row:
   { store/sku, kpi, value, chain_avg, z_score, gap_to_target, type }
```

### Why Directional Masking?

Without directional masking, a top-performing store with SPSF = ₹4,000 (z = +4.1) would be flagged as an anomaly and appear in the 🔴 Critical section — which is logically wrong. Directional masking ensures:
- SPSF anomalies = only stores far *below* average (underperformers)
- DOI anomalies = only stores far *above* average (overstock risk)
- Sales anomalies = only stores far *below* average (revenue drop)

---

## 20. Security Design

| Concern | Approach |
|---|---|
| SQL Injection | All SQL generated by LLM under strict rules; `validate_sql_basic()` blocks non-SELECT |
| API Key Storage | SQLite-backed settings store (no plaintext in code/env files) |
| ClickHouse Access | Read-only credentials, HTTPS (port 8443) |
| Dangerous SQL | Keyword blocklist: DROP, DELETE, TRUNCATE, ALTER, INSERT, UPDATE, CREATE |
| Auth | Session-based (OAuth integration hooks in main.py) |
| CORS | Open for on-prem (restrict to specific origins in production) |
| Data Sensitivity | No PII stored in LLM prompts (CUSTOMER_MOBILE hashed at query level) |

---

## 21. Performance Characteristics

| Metric | Value |
|---|---|
| ClickHouse main query (store MTD) | 2–5 seconds |
| Supplementary queries (4 queries) | 8–20 seconds total |
| LLM SQL generation | 2–4 seconds |
| LLM response (streaming start) | 3–6 seconds first token |
| Schema cache | 1 hour (avoid repeat schema loads) |
| Max prompt size | ~50,000 chars (main data + supplementary) |
| Max LLM response | 8,000 tokens |
| Alert scan (all stores) | 15–30 seconds |
| SQLite operations | < 10ms |

---

## 22. Deployment Architecture

### On-Premises Single Server

```
┌─────────────────────────────────────┐
│         On-Premises Server          │
│                                     │
│  ┌──────────────────────────────┐  │
│  │  Python venv (.venv)         │  │
│  │  uvicorn main:app            │  │
│  │  Port 8001                   │  │
│  └──────────────────────────────┘  │
│                                     │
│  ┌──────────────────────────────┐  │
│  │  SQLite: riect.db            │  │
│  │  (config, alerts, sessions)  │  │
│  └──────────────────────────────┘  │
│                                     │
│  Process managed by:               │
│  - macOS: launchd .plist           │
│  - Linux: systemd .service         │
└─────────────────────────────────────┘
         │                 │
         │                 │
   ┌─────▼──────┐   ┌──────▼─────────┐
   │  ClickHouse │   │  LLM Providers  │
   │  Remote     │   │  (Internet)     │
   │  (HTTPS)    │   │  or Ollama      │
   └────────────┘   └────────────────┘
```

### Startup Sequence

```bash
cd app/backend
source .venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8001
```

On startup:
1. `init_db()` — create SQLite tables if not exist
2. `run_live_scan()` — scan ClickHouse KPIs, populate alert inbox
3. App ready — serve frontend + WebSocket

---

## 23. Key Design Decisions

### Decision 1: Deterministic KPI Engines (no ML for KPI computation)
**Rationale**: SPSF, ST%, DOI are deterministic formulas. Using ML would introduce unexplainable variance. Python/pandas engines are auditable, debuggable, and correct.

### Decision 2: Pre-Aggregation for Sell-Through
**Problem**: Joining `inventory_current` × `pos_transactional_data` on ICODE without pre-grouping causes row multiplication — each inventory row matches multiple sales rows, inflating both SOH and QTY by different factors.
**Solution**: Both sides pre-aggregated (GROUP BY ICODE or STORE_CODE) in subqueries before joining. Result: accurate ST% ~44-46%, not 0.3% (row-multiplied) or 31% (SOH-filtered).

### Decision 3: Supplementary Queries (auto-fetch)
**Problem**: A single SQL query for store-level KPI never returns department, article, or hourly data. The LLM cannot generate sections it has no data for.
**Solution**: 5 additional pre-built ClickHouse queries run automatically after the main query, injected into the LLM prompt. No user action required.

### Decision 4: Directional Anomaly Masking
**Problem**: Standard z-score flags both extremes — top performers appeared in 🔴 Critical anomaly list.
**Solution**: Each KPI has a `bad_direction` property. Only flag z ≤ -2.0 for SPSF/Sales (low = bad), z ≥ +2.0 for DOI (high = bad). Top performers never appear as anomalies.

### Decision 5: Active Store Filter (everywhere)
**Rationale**: Closed stores have sales history but no current operational relevance. Mixing their data with live stores corrupts KPI averages. The filter is applied at every ClickHouse query boundary.

### Decision 6: Scrollable Table Wrappers (not pagination)
**Rationale**: Pagination fragments analysis. A user comparing 10 stores needs all 10 visible in context. Scrollable max-height containers preserve context while fitting the screen.

### Decision 7: max_tokens=8000 for Analysis
**Rationale**: 8-section responses with 10-row tables, 3-store insight blocks, IST/Markdown guidance, and peak hours for all stores require 6,000–8,000 tokens. At 4,000 tokens, the LLM truncates sections mid-table.

### Decision 8: Indian FY Date Engine (not calendar year)
**Problem**: Standard `date_trunc('year', ...)` → Jan 1 is meaningless for Indian retail. YTD queries against calendar year misrepresent performance for Apr–Mar businesses.
**Solution**: `date_engine.py` resolves all temporal contexts (YTD, MTD, WTD, QTD, LTL) to Indian FY (Apr 1 – Mar 31). All period SQL is generated with correct FY start/end dates, FY week numbers, and prior-FY same-period alignment for LTL comparisons.

### Decision 9: 13-KPI Registry with Dynamic Availability Detection
**Problem**: Not all queries return data sufficient for all 13 KPIs. Generating KPI sections for unavailable metrics produces N/A-filled tables and hallucination.
**Solution**: `kpi_alignment.py` auto-detects which KPIs are computable from the available DataFrame columns. The prompt builder injects only available KPI sections, and the LLM receives an explicit `KPI AVAILABILITY MAP` so it never fabricates values for absent metrics.

### Decision 10: Product Alignment Cache (ClickHouse → SQLite)
**Problem**: Every product lookup against ClickHouse adds 1–3 seconds latency and consumes ClickHouse query slots for static master data that rarely changes.
**Solution**: `product_alignment` SQLite table caches article/division/section/department/cost/MRP data locally. Lookups are sub-millisecond. Cache is refreshed on demand via `POST /api/products/sync`. Indexed on division, section, department for fast category filtering.

---

*DSR|RIECT — Built by Dinesh Srivastava | Proprietary Retail Intelligence Platform*
