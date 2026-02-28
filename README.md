# DSR|RIECT — Retail Intelligence Execution Control Tower

> **"See every exception. Know every risk. Decide with evidence. Act with confidence."**

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115%2B-green)](https://fastapi.tiangolo.com)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-Live-orange)](https://clickhouse.com)
[![LLM](https://img.shields.io/badge/LLM-Claude%20%7C%20OpenAI%20%7C%20Gemini%20%7C%20Qwen%20%7C%20Ollama-purple)](https://anthropic.com)
[![License](https://img.shields.io/badge/License-Proprietary-red)](LICENSE)

---

## Table of Contents

1. [What is DSR|RIECT?](#1-what-is-dsrriect)
2. [Business KPI Targets](#2-business-kpi-targets)
3. [Core Operating Principle](#3-core-operating-principle)
4. [Key Capabilities](#4-key-capabilities)
5. [Tech Stack](#5-tech-stack)
6. [Project Structure](#6-project-structure)
7. [Quick Start](#7-quick-start)
8. [Configuration](#8-configuration)
9. [API Reference](#9-api-reference)
10. [LLM Support](#10-llm-support)
11. [KPI Engine Reference](#11-kpi-engine-reference)
12. [Alert Priority System](#12-alert-priority-system)
13. [Chatbot Pipeline](#13-chatbot-pipeline)
14. [Roadmap](#14-roadmap)

---

## 1. What is DSR|RIECT?

**DSR|RIECT** (Retail Intelligence Execution Control Tower) is a production-grade, on-premises AI system that transforms raw retail data into prioritised, time-bound execution intelligence.

It is not a reporting tool. It is not a dashboard. It is a **decision engine** — purpose-built to detect retail performance anomalies, rank exceptions by business impact, and generate specific, owner-assigned action playbooks.

### The Problem It Solves

Retail operations generate enormous amounts of data across hundreds of stores, thousands of SKUs, and multiple channels. Traditional BI tools surface *what happened*. RIECT surfaces *what to do about it — and by when*.

| Traditional BI | DSR|RIECT |
|---|---|
| Shows sales dashboards | Detects who missed target and why |
| Requires analyst to find exceptions | Auto-generates ranked P1→P4 exception inbox |
| Reports on the past | Projects forward trajectory |
| Gives data to decision makers | Gives decisions with evidence |
| Weekly/monthly cadence | Live ClickHouse + daily scan |

---

## 2. Business KPI Targets

RIECT is built around four measurable retail KPIs:

| KPI | Full Name | Direction | Target | Thresholds |
|---|---|---|---|---|
| **SPSF** | Sales Per Square Foot | ↑ Maximise | ₹1,000/sqft/month | P1<₹500 · P2<₹750 · P3<₹1,000 |
| **ST%** | Sell-Through % | ↑ Maximise | 95% | P1<60% · P2<80% · P3<95% |
| **DOI** | Days of Inventory | ↓ Minimise | ≤15 days | P1>90d · P2>60d · P3>30d |
| **UPT** | Units Per Transaction | ↑ Maximise | 2.5 | P1<1.2 · P2<1.5 · P3<2.0 |

Every alert, recommendation, and chatbot response is anchored to these four targets.

---

## 3. Core Operating Principle

```
SEE  →  KNOW  →  DECIDE  →  ACT  →  TRACK
```

| Stage | RIECT Capability |
|---|---|
| **SEE** | Live KPI tiles, Control Tower dashboard, ClickHouse real-time query |
| **KNOW** | Anomaly detection (z-score), threshold breaches, P1–P4 ranking |
| **DECIDE** | AI chatbot with multi-LLM routing, evidence-grounded recommendations |
| **ACT** | Action playbooks with owner + timeline + IST/Markdown guidance |
| **TRACK** | Session-persistent chat history, alert lifecycle in SQLite |

---

## 4. Key Capabilities

### Live KPI Dashboard
- Chain-level SPSF, Sell-Through %, DOI, UPT, ATV, Discount Rate — refreshed on demand
- Per-store priority tiles (P1 🔴 → P2 🟠 → P3 🟡 → On Target 🟢)
- Auto-scan every startup with configurable re-scan

### Intelligent Alert / Exception Engine
- Threshold-based breach detection across all active stores
- Statistical anomaly detection (z-score, directional — underperformers for SPSF/ST, overstock for DOI)
- IST (Inter Store Transfer) vs Markdown decision framework per anomaly
- Alert inbox: P1/P2/P3 ranked with root cause + top 3 actions + owner + timeline

### AI Chatbot — Full Retail Intelligence
- Natural language query → ClickHouse SQL → structured analysis
- 8-section comprehensive report: Executive Summary → KPI Scorecard → Store Performance → Dept/Articles → MRP → Anomalies → Peak Hours → Priority Actions
- Supplementary auto-queries: dept breakdown, articles, peak hours, top MRP — all auto-fetched
- Inline ATV, UPT computation from raw data — no pre-computed columns required
- Multi-LLM routing: Claude Sonnet, OpenAI GPT-4, Gemini, Qwen, Ollama (local)

### Store Performance Analysis
- Top 10 + Bottom 10 stores by SPSF/sales with Region, Zone, SPSF, ATV, Bills, UPT
- Detailed per-store insight blocks: what's working, threats, what to replicate, which stores to target
- Anomaly direction-aware: only flags underperformers for SPSF/Sales, overstock for DOI

### Department & Article Intelligence
- Top 10 + Bottom 10 departments by net sales with discount%, article count
- Top 10 + Bottom 10 articles with STYLE_OR_PATTERN, SIZE, COLOR, avg MRP, sell-through
- Slowest movers identified for markdown/clearance recommendation

### Peak Hours Analysis
- Per-store, per-hour transaction count + unique customer count (bill-based + mobile-based)
- Chain-wide peak window identification (≥50% of peak traffic)
- Staffing, replenishment, and promotion timing recommendations

---

## 5. Tech Stack

| Layer | Technology |
|---|---|
| **Backend** | Python 3.10+, FastAPI, WebSocket streaming |
| **Database (Analytics)** | ClickHouse (remote — `vmart_sales`, `vmart_product` schemas) |
| **Database (Config/Alerts)** | SQLite (`riect.db`) |
| **LLM** | Claude Sonnet 4.6, OpenAI GPT-4o, Gemini 1.5 Pro, Qwen3, Ollama (local) |
| **Frontend** | Vanilla JS + marked.js, WebSocket client, CSS variables |
| **KPI Engines** | Python (pandas, numpy) — deterministic, no ML black box |
| **Anomaly Detection** | Z-score with directional masking per KPI |
| **Embeddings** | sentence-transformers (for schema vectorisation) |
| **Deployment** | On-premises, systemd / launchd, no cloud dependency |

---

## 6. Project Structure

```
DSR|RIECT/
├── app/
│   ├── backend/
│   │   ├── main.py                    # FastAPI app entry point
│   │   ├── config.py                  # KPI thresholds, app settings
│   │   ├── db.py                      # SQLite init & connection
│   │   ├── requirements.txt
│   │   │
│   │   ├── clickhouse/
│   │   │   ├── connector.py           # ClickHouse client factory
│   │   │   ├── query_runner.py        # Safe query executor
│   │   │   └── schema_inspector.py   # Schema discovery & caching
│   │   │
│   │   ├── llm/
│   │   │   ├── llm_router.py          # Multi-LLM router (Claude/OpenAI/Gemini/Qwen/Ollama)
│   │   │   ├── cloud_client.py        # Anthropic + OpenAI + Gemini client
│   │   │   ├── qwen_client.py         # Qwen cloud client
│   │   │   └── ollama_client.py       # Local Ollama client
│   │   │
│   │   ├── pipeline/
│   │   │   ├── orchestrator.py        # Master pipeline: route → SQL → KPI → LLM → stream
│   │   │   ├── intent_engine.py       # Query intent classification (regex + keyword)
│   │   │   ├── query_normalizer.py    # Date resolution, typo fix, store name normalisation
│   │   │   ├── context_builder.py     # Builds context dict (schema, history, hints)
│   │   │   ├── sql_generator.py       # LLM → ClickHouse SQL with rules + validation
│   │   │   ├── prompt_builder.py      # Final LLM prompt assembly (8-section format)
│   │   │   ├── vectoriser.py          # Schema table ranking by semantic similarity
│   │   │   └── response_formatter.py # Structures final response blocks
│   │   │
│   │   ├── riect/
│   │   │   ├── kpi_engine/
│   │   │   │   ├── kpi_controller.py  # Runs all KPI engines, returns unified result
│   │   │   │   ├── spsf_engine.py     # Sales Per Square Foot compute + breach detection
│   │   │   │   ├── sell_thru_engine.py# Sell-Through % (item-level pre-agg, no row multiply)
│   │   │   │   ├── doi_engine.py      # Days of Inventory + Days of Cover
│   │   │   │   ├── mbq_engine.py      # Minimum Buy Quantity compliance
│   │   │   │   └── anomaly_engine.py  # Z-score anomaly detection (directional)
│   │   │   │
│   │   │   ├── alert_engine/
│   │   │   │   ├── live_scanner.py    # Startup/on-demand ClickHouse KPI scan
│   │   │   │   ├── priority_engine.py # P1/P2/P3/P4 classification rules
│   │   │   │   ├── alert_generator.py # KPI breach → AlertRecord objects
│   │   │   │   ├── action_recommender.py # Alert → action playbook enrichment
│   │   │   │   └── alert_store.py     # SQLite read/write for alerts
│   │   │   │
│   │   │   └── api/
│   │   │       ├── kpi_api.py         # GET /api/kpi/live, GET /api/kpi/riect
│   │   │       └── alerts_api.py      # GET/POST /api/alerts/*
│   │   │
│   │   └── settings/
│   │       ├── settings_store.py      # ClickHouse config, LLM keys (SQLite-backed)
│   │       ├── store_sqft_store.py    # Store floor sqft lookup (CSV import → SQLite)
│   │       └── riect_plan_store.py    # KPI targets, plan config
│   │
│   └── frontend/
│       ├── index.html                 # Single-page app shell
│       ├── app.js                     # WebSocket client, markdown render, UI logic
│       └── styles.css                 # Dark theme, scrollable tables, KPI tiles
│
├── ARCHITECTURE.md                    # Technical deep-dive
├── README.md                          # This file
├── RIECT-PLAN.md                      # Strategic roadmap
├── RIECT-BACKEND-ENGINE.md            # Engine reference
├── RIECT-ALERT-ENGINE-ARCHITECTURE.md # Alert engine design
├── RIECT-CONTROL-TOWER-UI.md          # UI/UX specification
└── RIECT-DATA-INTEGRATION.md          # Data integration guide
```

---

## 7. Quick Start

### Prerequisites

- Python 3.10+
- ClickHouse instance (or credentials to remote cluster)
- At least one LLM API key (Claude, OpenAI, Gemini) **or** Ollama installed locally

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/dineshsrivastava07-cell/DSR-RIECT-AI.git
cd DSR-RIECT-AI

# 2. Create virtual environment
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r app/backend/requirements.txt

# 4. Start the backend
cd app/backend
uvicorn main:app --host 0.0.0.0 --port 8001 --reload

# 5. Open the UI
# Navigate to http://localhost:8001 in your browser
```

### First-Run Setup (via UI Settings ⚙)

1. **ClickHouse**: Enter host, port, username, password → Test Connection
2. **LLM**: Add at least one API key (Claude recommended) and set as default
3. **Store SQFT**: Upload CSV with `store_id, floor_sqft` columns for SPSF computation
4. The system auto-scans KPIs on startup — P1/P2/P3 alerts appear in the exception inbox

---

## 8. Configuration

### Environment Variables (optional `.env`)

```bash
ANTHROPIC_API_KEY=sk-ant-...      # Claude (recommended)
OPENAI_API_KEY=sk-...             # OpenAI GPT-4o
GOOGLE_API_KEY=...                # Gemini Pro
```

### KPI Thresholds (`app/backend/config.py`)

```python
SPSF_THRESHOLDS   = {"P1": 500,  "P2": 750,  "P3": 1000, "target": 1000}
SELL_THRU_THRESHOLDS = {"P1": 0.60, "P2": 0.80, "P3": 0.95, "target": 0.95}
DOI_THRESHOLDS    = {"P1": 90,   "P2": 60,   "P3": 30,   "target": 15}
UPT_THRESHOLDS    = {"P1": 1.2,  "P2": 1.5,  "P3": 2.0,  "target": 2.5}
```

### Store SQFT CSV Format

```csv
store_id,floor_sqft
101,4500
102,3200
103,6800
```

---

## 9. API Reference

| Method | Endpoint | Description |
|---|---|---|
| `WS` | `/ws/chat` | WebSocket — real-time chatbot with streaming |
| `GET` | `/api/kpi/live` | Live chain-level KPI snapshot (ClickHouse) |
| `GET` | `/api/kpi/riect` | KPI dashboard config (thresholds + alert counts) |
| `GET` | `/api/alerts/` | Fetch active alerts (P1/P2/P3 ranked) |
| `POST` | `/api/alerts/scan` | Trigger on-demand live KPI scan |
| `GET` | `/api/alerts/summary` | Alert counts by priority and KPI type |
| `POST` | `/api/settings/clickhouse` | Save ClickHouse connection config |
| `POST` | `/api/settings/llm` | Save LLM API key |
| `GET` | `/api/schema/summary` | ClickHouse schema overview |
| `POST` | `/api/sqft/import` | Import store floor sqft from CSV |

### WebSocket Message Protocol

```jsonc
// Client → Server
{ "query": "Show top 10 stores by SPSF this month", "session_id": "abc123" }

// Server → Client (streaming events)
{ "type": "stage",       "stage": "sql_generate" }
{ "type": "sql_generated","sql": "SELECT ..." }
{ "type": "data_ready",  "rows": 187, "columns": [...] }
{ "type": "kpi_done",    "p1": 12, "p2": 28, "p3": 41 }
{ "type": "token",       "content": "## Executive Summary\n..." }   // streaming
```

---

## 10. LLM Support

| Provider | Models | Use Case |
|---|---|---|
| **Anthropic Claude** | claude-sonnet-4-6 | Recommended — best analytical reasoning |
| **OpenAI** | gpt-4o, gpt-4-turbo | Strong SQL generation |
| **Google Gemini** | gemini-1.5-pro, gemini-2.0-flash | Cost-effective high-volume |
| **Qwen** | qwen3-coder-flash, qwen3.5-plus | Fast, low-cost cloud |
| **Ollama** | llama3.1, mistral, etc. | Fully offline / air-gapped |

The LLM router selects the configured default. All LLM calls use:
- **SQL generation**: temperature 0.1, max_tokens 1500
- **Analytical response**: temperature 0.3, max_tokens 8000

---

## 11. KPI Engine Reference

### SPSF Engine (`spsf_engine.py`)
- **Formula**: `MTD Net Sales ÷ Floor SQFT × (Days in Month ÷ Days Elapsed)`
- **Input**: Store-level MTD sales + floor sqft lookup
- **Output**: Monthly-projected SPSF per store, P1/P2/P3 breach list
- **Rule**: Stores with floor sqft < 300 excluded (kiosk filter)

### Sell-Through Engine (`sell_thru_engine.py`)
- **Formula**: `COALESCE(SUM(QTY),0) / (COALESCE(SUM(QTY),0) + SUM(SOH)) × 100` per ICODE
- **Chain level**: `avgIf(st_pct, st_pct > 0)` across all ICODEs
- **Critical rule**: No `WHERE SOH > 0` filter — sold-out items (SOH=0 + sales) = 100% ST%
- **Pre-aggregation**: Both sides (inventory + sales) grouped by ICODE before joining to prevent row multiplication

### DOI Engine (`doi_engine.py`)
- **Formula**: `SOH ÷ (MTD_QTY ÷ days_elapsed)` per store
- **Store level**: Pre-aggregated subqueries — inventory by STORE_CODE, sales by STORE_ID
- **Chain level**: `sumIf(icode_soh, icode_qty > 0) / sumIf(icode_qty, icode_qty > 0)`

### Anomaly Engine (`anomaly_engine.py`)
- **Method**: Z-score per KPI column across store dataset
- **Threshold**: Z ≥ 2.0 (P2 anomaly), Z ≥ 3.0 (P1 critical)
- **Directional masking**: Only flags low z for SPSF/UPT/Sales (underperformers); only high z for DOI (overstock). Prevents top performers appearing in anomaly list.

---

## 12. Alert Priority System

```
P1 🔴  Critical — Act Today    → Zone Manager escalation, immediate IST/markdown trigger
P2 🟠  High     — This Week    → Category team review, replenishment or promotion
P3 🟡  Medium   — This Month   → Monitor, plan ahead, adjust buying
P4 🟢  Watch    — Low Risk     → No action needed now, flag for next review
```

### IST vs Markdown Decision Framework

| DOI | ST% | Decision |
|---|---|---|
| >90d (P1) | <40% | 🔴 **MARKDOWN** — dead stock |
| >60d (P2) | <60% | 🟠 **IST** to high-velocity same-Zone store + partial markdown |
| >30d (P3) | <80% | 🟡 **IST** to nearest store with DOI <15d in same Region |
| <30d | <60% | 🔵 **PROMO PUSH** — fresh stock not moving |
| >90d | >60% | 🟠 **IST** to stores showing low SOH in same Zone |

---

## 13. Chatbot Pipeline

Every user message flows through an 11-stage pipeline:

```
User Query
    │
    ▼
[1] Intent Classification     → Route: KPI_ANALYSIS / DATA_QUERY / PEAK_HOURS / ...
    │
    ▼
[2] Query Normalisation       → Resolve dates, fix typos, extract target_date
    │
    ▼
[3] Schema Load               → ClickHouse schema discovery (cached 1hr)
    │
    ▼
[4] Data Freshness Check      → Find latest complete sales date (≥10,000 bills)
    │
    ▼
[5] SQL Generation (LLM)      → ClickHouse SQL with rules, active-store filter
    │
    ▼
[6] SQL Execute + Auto-Retry  → Run query, self-correct on error
    │
    ▼
[7] Supplementary Queries     → Auto-fetch dept, articles, peak hours, top MRP
    │
    ▼
[8] KPI Engines               → SPSF, ST%, DOI, UPT, Anomaly detection
    │
    ▼
[9] Alert Generation          → P1/P2/P3 alerts with action playbooks
    │
    ▼
[10] Prompt Build             → 8-section analytical prompt + all data injected
    │
    ▼
[11] LLM Stream               → Streaming response to WebSocket → UI
```

### Active Store Rule (Applied Everywhere)
All queries automatically exclude closed stores:
```sql
STORE_ID NOT IN (SELECT CODE FROM vmart_sales.stores WHERE CLOSING_DATE IS NOT NULL)
```

---

## 14. Roadmap

| Phase | Capability | Status |
|---|---|---|
| **Phase 1** | Alert/Exception Engine, KPI detection, chatbot | ✅ Live |
| **Phase 2** | Control Tower Dashboard (dual-pane: KPI tiles + chat) | ✅ Live |
| **Phase 3** | Real-time signal ingestion, scheduled scans | 🔄 In Progress |
| **Phase 4** | Execution loop — playbooks, escalation, outcome tracking | 📋 Planned |
| **Phase 5** | Forecast deviation, forward DOI projection | 📋 Planned |
| **Phase 6** | Vendor risk signals, supply chain intelligence | 📋 Planned |

---

## Author

**Dinesh Srivastava**
Retail Intelligence & AI Systems
[GitHub: dineshsrivastava07-cell](https://github.com/dineshsrivastava07-cell)

---

*DSR|RIECT is a proprietary retail intelligence platform. All rights reserved.*
