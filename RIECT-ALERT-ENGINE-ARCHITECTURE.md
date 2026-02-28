# RIECT Alert / Exception Engine — Architecture Plan
## DSR|KRAI — Phase 1 Completion

**Version:** 1.0
**Date:** 2026-02-20
**Status:** Approved for Build
**Owner:** Dinesh Srivastava

---

## 1. Purpose

This document defines the technical architecture for two Phase 1 RIECT capabilities:

1. **RIECT KPI Detection Engine** — Computes SPSF, Sell-Thru %, DOI, Days of Cover, and MBQ Compliance from uploaded retail data. Evidence-grounded, Python-first, deterministic.

2. **Alert / Exception Engine** — Consumes KPI output, applies P1–P4 priority classification rules, generates ranked action recommendations, persists to DB, and exposes `/api/alerts` endpoint.

Together these deliver the **KNOW** stage of RIECT:
```
SEE → [KNOW] → DECIDE → ACT → TRACK
```

---

## 2. Position in DSR|KRAI Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        EXISTING DSR|KRAI STACK                          │
│                                                                         │
│  Frontend (HTML/JS/WS)  →  main.py (FastAPI/WS)  →  LLM Router         │
│                              ↓                                          │
│              Analytics Pipeline (analysis/controller.py)                │
│                    ↓                    ↓                               │
│             File Context            KPI Modules (kpi/)                  │
│                                         ↓                               │
│                          ┌──────────────────────────────┐               │
│                          │      RIECT MODULE (NEW)       │               │
│                          │                              │               │
│                          │  kpi_engine/ ──→ alert_engine/│               │
│                          │        ↓               ↓     │               │
│                          │  /api/kpi/riect   /api/alerts │               │
│                          └──────────────────────────────┘               │
│                                         ↓                               │
│                          riect_alerts table (SQLite DB)                  │
│                                         ↓                               │
│                     Alert Panel in Frontend (above chat)                │
└─────────────────────────────────────────────────────────────────────────┘
```

**Integration points:**
- Reads from: `analysis/controller.py` output DataFrames and uploaded file data
- Writes to: `riect_alerts` table in `dsr_krai.db` (via `db.py`)
- Serves: `/api/alerts` and `/api/kpi/riect` REST endpoints (via `main.py`)
- Renders: Alert panel block in frontend WebSocket `done` message (`blocks` array)

---

## 3. Module Structure

```
backend/riect/
├── __init__.py
│
├── kpi_engine/
│   ├── __init__.py
│   ├── spsf_engine.py          # SPSF computation (Sales ÷ Floor Space)
│   ├── sell_thru_engine.py     # Sell-Thru % computation
│   ├── doi_engine.py           # DOI + Days of Cover computation
│   ├── mbq_engine.py           # MBQ compliance check (store vs MBQ floor)
│   └── kpi_controller.py       # Orchestrator — runs all KPI engines on a DataFrame
│
├── alert_engine/
│   ├── __init__.py
│   ├── priority_engine.py      # P1/P2/P3/P4 classification rules
│   ├── alert_generator.py      # Generates AlertRecord objects from KPI breaches
│   ├── action_recommender.py   # Maps each alert type → recommended action + timeline
│   └── alert_store.py          # DB read/write for riect_alerts table
│
└── api/
    ├── __init__.py
    ├── alerts_api.py           # GET /api/alerts, POST /api/alerts/run
    └── kpi_api.py              # GET /api/kpi/riect
```

---

## 4. KPI Detection Engine

### 4.1 SPSF Engine (`kpi_engine/spsf_engine.py`)

**Formula:**
```
SPSF = Net Sales Amount ÷ Floor Space (sq ft)
```

**Target:** ≥ 1,000

**Inputs required:**
| Field | Source Entity |
|-------|--------------|
| `net_sales_amount` | Sales Data |
| `store_id` | Sales Data / Store Master |
| `floor_space_sqft` | Store Master |
| `article_id` (optional) | For article-level SPSF |
| `period` (week/month) | Sales Data |

**Output columns added to DataFrame:**
```python
{
  "spsf": float,                    # computed SPSF value
  "spsf_target": 1000.0,            # fixed target
  "spsf_gap": float,                # spsf - spsf_target (negative = shortfall)
  "spsf_pct_of_target": float,      # (spsf / 1000) * 100
  "spsf_status": str,               # "CRITICAL" | "HIGH" | "MEDIUM" | "OK"
}
```

**Status thresholds:**
```
spsf < 500      → CRITICAL (P1)
500 ≤ spsf < 750 → HIGH (P2)
750 ≤ spsf < 1000 → MEDIUM (P3)
spsf ≥ 1000     → OK
```

**Aggregation levels supported:**
- Store × Period
- Category × Store × Period
- Region × Period (roll-up via store hierarchy)

---

### 4.2 Sell-Thru Engine (`kpi_engine/sell_thru_engine.py`)

**Formula:**
```
Sell-Thru % = (Net Sales Qty ÷ (Net Sales Qty + Inventory Qty)) × 100
```

**Target:** ≥ 95%

**Inputs required:**
| Field | Source Entity |
|-------|--------------|
| `net_sales_qty` | Sales Data |
| `inventory_qty` | Inventory (Store SOH) |
| `article_id` / `option_id` / `sku_id` | Article Master |
| `store_id` | Store Master |
| `season_code` / `weeks_on_floor` | Trading Calendar (future) |

**Output columns:**
```python
{
  "sell_thru_pct": float,           # computed sell-thru %
  "sell_thru_target": 95.0,         # fixed target
  "sell_thru_gap": float,           # sell_thru_pct - 95.0
  "sell_thru_status": str,          # "CRITICAL" | "HIGH" | "MEDIUM" | "OK"
  "markdown_trigger": bool,         # True if sell_thru < 80% and weeks_on_floor > 6
}
```

**Status thresholds:**
```
sell_thru < 60%     → CRITICAL (P1) — immediate markdown
60% ≤ sell_thru < 80% → HIGH (P2) — promotional push
80% ≤ sell_thru < 95% → MEDIUM (P3) — monitor trajectory
sell_thru ≥ 95%     → OK
```

**Markdown trigger logic:**
- `sell_thru_pct < 80%` AND `weeks_on_floor > 6` → `markdown_trigger = True`
- Weeks on floor derived from Trading Calendar if available; else from data arrival date

---

### 4.3 DOI Engine (`kpi_engine/doi_engine.py`)

**Dual computation:**

**DOI (backward — actual performance):**
```
DOI = (Store Inventory Qty + GIT Qty) ÷ Avg Daily Net Sales Qty (4-week rolling)
```

**Days of Cover (forward — trajectory):**
```
Days of Cover = (Store Inventory Qty + GIT Qty) ÷ Forecasted Daily Sales Rate
```

**Inputs required:**
| Field | Source Entity |
|-------|--------------|
| `inventory_qty` | Inventory (Store SOH) |
| `git_qty` | GIT (Goods in Transit) |
| `net_sales_qty` | Sales Data (4-week history) |
| `forecast_daily_rate` | Forecast Data (if available) |
| `article_id`, `store_id` | Masters |

**Output columns:**
```python
{
  "doi": float,                     # days of inventory (backward)
  "days_of_cover": float,           # days of cover (forward, if forecast available)
  "doi_status": str,                # "CRITICAL" | "HIGH" | "MEDIUM" | "OK"
  "overstock_flag": bool,           # doi > 90
  "stockout_risk_days": int,        # days until stockout (doi if < reorder threshold)
  "reorder_trigger": bool,          # True if doi < 14 days (configurable)
}
```

**Status thresholds:**
```
doi > 90 days       → CRITICAL (P1) — halt reorder, initiate clearance
60 < doi ≤ 90 days  → HIGH (P2) — halt reorder review
30 < doi ≤ 60 days  → MEDIUM (P3) — watch
doi ≤ 30 days       → OK (or trigger reorder if < threshold)
doi < 14 days       → reorder_trigger = True (separate from exception)
```

---

### 4.4 MBQ Engine (`kpi_engine/mbq_engine.py`)

**Formula:**
```
MBQ Compliance = Store Inventory Qty ≥ Article MBQ (by store)
```

**Inputs required:**
| Field | Source Entity |
|-------|--------------|
| `inventory_qty` | Inventory (Store SOH) |
| `mbq_qty` | Article MBQ by Store |
| `article_id`, `store_id` | Masters |

**Output columns:**
```python
{
  "mbq_qty": int,                   # minimum base quantity floor
  "mbq_shortfall": int,             # max(0, mbq_qty - inventory_qty)
  "mbq_compliant": bool,            # inventory_qty >= mbq_qty
  "so_trigger": bool,               # same as not mbq_compliant (triggers SO)
}
```

---

### 4.5 KPI Controller (`kpi_engine/kpi_controller.py`)

Orchestrates all four engines. Entry point called by `/api/kpi/riect` and Alert Engine.

```python
def run_riect_kpis(
    sales_df: pd.DataFrame,
    inventory_df: pd.DataFrame,
    store_master_df: pd.DataFrame,
    article_master_df: pd.DataFrame,
    git_df: Optional[pd.DataFrame] = None,
    forecast_df: Optional[pd.DataFrame] = None,
    mbq_df: Optional[pd.DataFrame] = None,
) -> RIECTKPIResult:
    """
    Run all RIECT KPI engines.
    Returns RIECTKPIResult with:
      - spsf_df: DataFrame with SPSF columns
      - sell_thru_df: DataFrame with Sell-Thru columns
      - doi_df: DataFrame with DOI columns
      - mbq_df: DataFrame with MBQ columns
      - summary: Dict with aggregate KPI summary
    """
```

**Graceful degradation:** If a required DataFrame is missing (e.g., no GIT data), the engine skips GIT contribution and logs a data gap warning. Never crashes.

---

## 5. Alert / Exception Engine

### 5.1 Data Model — AlertRecord

```python
@dataclass
class AlertRecord:
    alert_id: str           # UUID
    created_at: datetime    # IST
    priority: str           # P1 | P2 | P3 | P4
    kpi_type: str           # SPSF | SELL_THRU | DOI | MBQ | FORECAST_DEV
    signal_type: str        # ANOMALY | FORECAST_DEVIATION | RISK
    dimension: str          # store_id or article_id or category
    dimension_value: str    # e.g., "Store_12" or "SKU_A47"
    kpi_value: float        # actual KPI computed value
    threshold: float        # threshold that was breached
    gap: float              # kpi_value - threshold (magnitude of breach)
    status: str             # CRITICAL | HIGH | MEDIUM | MONITORING
    exception_text: str     # human-readable: "Store 12 SPSF dropped to 423"
    recommended_action: str # "Floor re-plan" / "Markdown 20%" etc.
    action_owner: str       # "Store Manager" | "Regional" | "Buyer" | "HQ"
    response_timeline: str  # "Today" | "Within 3 days" | "This week" | "Next review"
    expected_impact: str    # qualitative impact statement
    session_id: str         # which data upload session triggered this
    resolved: bool          # False initially; True when action taken
    resolved_at: Optional[datetime]
```

---

### 5.2 Priority Engine (`alert_engine/priority_engine.py`)

Applies business rules to classify each KPI breach:

```python
PRIORITY_RULES = {
    "SPSF": [
        {"condition": lambda v: v < 500,          "priority": "P1", "status": "CRITICAL"},
        {"condition": lambda v: 500 <= v < 750,   "priority": "P2", "status": "HIGH"},
        {"condition": lambda v: 750 <= v < 1000,  "priority": "P3", "status": "MEDIUM"},
        {"condition": lambda v: v >= 1000,         "priority": "P4", "status": "OK"},
    ],
    "SELL_THRU": [
        {"condition": lambda v: v < 60,           "priority": "P1", "status": "CRITICAL"},
        {"condition": lambda v: 60 <= v < 80,     "priority": "P2", "status": "HIGH"},
        {"condition": lambda v: 80 <= v < 95,     "priority": "P3", "status": "MEDIUM"},
        {"condition": lambda v: v >= 95,           "priority": "P4", "status": "OK"},
    ],
    "DOI": [
        {"condition": lambda v: v > 90,           "priority": "P1", "status": "CRITICAL"},
        {"condition": lambda v: 60 < v <= 90,     "priority": "P2", "status": "HIGH"},
        {"condition": lambda v: 30 < v <= 60,     "priority": "P3", "status": "MEDIUM"},
        {"condition": lambda v: v <= 30,           "priority": "P4", "status": "OK"},
    ],
    "DAYS_OF_COVER": [
        {"condition": lambda v: v < 7,            "priority": "P1", "status": "CRITICAL"},
        {"condition": lambda v: 7 <= v < 14,      "priority": "P2", "status": "HIGH"},
        {"condition": lambda v: 14 <= v < 21,     "priority": "P3", "status": "MEDIUM"},
        {"condition": lambda v: v >= 21,           "priority": "P4", "status": "OK"},
    ],
}
```

**Compound rule — P1 upgrade:**
If any single dimension (store/SKU) has **two or more** P2 breaches simultaneously (e.g., both Sell-Thru HIGH and DOI HIGH), auto-upgrade to P1.

---

### 5.3 Alert Generator (`alert_engine/alert_generator.py`)

Takes KPI output DataFrames → produces list of `AlertRecord` objects.

```python
def generate_alerts(
    kpi_result: RIECTKPIResult,
    session_id: str,
    min_priority: str = "P3",      # P4 alerts = OK, suppress unless requested
) -> List[AlertRecord]:
    """
    Scan all KPI DataFrames for threshold breaches.
    Returns list of AlertRecords sorted by priority (P1 first), then gap magnitude.
    Deduplicates: one alert per dimension per kpi_type per session.
    """
```

**Alert text templates:**
```python
ALERT_TEMPLATES = {
    "SPSF_P1": "Store {dim} SPSF at {value:.0f} — {gap:.0f} below P1 threshold (500)",
    "SPSF_P2": "Store {dim} SPSF at {value:.0f} — below target 1,000 ({gap:.0f} gap)",
    "SELL_THRU_P1": "{dim} Sell-Thru at {value:.1f}% — CRITICAL (target 95%)",
    "SELL_THRU_P2": "{dim} Sell-Thru at {value:.1f}% — markdown risk in {weeks} weeks",
    "DOI_P1": "{dim} DOI = {value:.0f} days — OVERSTOCK (target: minimise)",
    "DOI_P2": "{dim} DOI = {value:.0f} days — above 60-day High threshold",
    "DOC_P1": "{dim} Days of Cover = {value:.0f} — STOCKOUT RISK within 1 week",
    "MBQ_BREACH": "{dim} at Store {store} — inventory {inv} below MBQ {mbq} (shortfall {gap})",
}
```

---

### 5.4 Action Recommender (`alert_engine/action_recommender.py`)

Maps each alert type + priority → recommended action, owner, timeline, and expected impact.

```python
ACTION_PLAYBOOK = {
    ("SPSF", "P1"): {
        "action": "Immediate floor re-plan or space reallocation",
        "owner": "Store Manager + Regional",
        "timeline": "Today",
        "impact": "Recover SPSF toward 500 floor within 2 weeks",
    },
    ("SPSF", "P2"): {
        "action": "Run promotional push to drive footfall + conversion",
        "owner": "Store Manager",
        "timeline": "Within 3 days",
        "impact": "Recover SPSF 50–100 pts within 4 weeks",
    },
    ("SPSF", "P3"): {
        "action": "Review space allocation vs. category contribution",
        "owner": "Merchandising",
        "timeline": "This week",
        "impact": "Trajectory correction toward 1,000 target",
    },
    ("SELL_THRU", "P1"): {
        "action": "Markdown 20–30% immediately. Evaluate inter-store transfer",
        "owner": "Buyer + Store Manager",
        "timeline": "Today",
        "impact": "Prevent dead stock, recover capital",
    },
    ("SELL_THRU", "P2"): {
        "action": "Promotional push — bundle/offer within 1 week",
        "owner": "Buyer",
        "timeline": "Within 3 days",
        "impact": "Accelerate Sell-Thru toward 95% by season end",
    },
    ("DOI", "P1"): {
        "action": "HALT reorder immediately. Initiate clearance or transfer",
        "owner": "Buyer + Replenishment",
        "timeline": "Today",
        "impact": "Reduce DOI below 60 days within 4 weeks",
    },
    ("DOI", "P2"): {
        "action": "Review and pause reorder. Flag for next planning cycle",
        "owner": "Replenishment",
        "timeline": "Within 3 days",
        "impact": "Prevent DOI escalating to P1",
    },
    ("DAYS_OF_COVER", "P1"): {
        "action": "Emergency replenishment SO. Check GIT and pending SO status",
        "owner": "Replenishment + Store Ops",
        "timeline": "Today",
        "impact": "Prevent stockout within 7 days",
    },
    ("MBQ", "P2"): {
        "action": "Raise SO for MBQ shortfall — check Pending SO queue first",
        "owner": "Replenishment",
        "timeline": "Within 3 days",
        "impact": "Maintain full floor presence",
    },
}
```

---

### 5.5 Alert Store (`alert_engine/alert_store.py`)

Thin wrapper over `db.py` for riect_alerts table operations.

```python
def save_alerts(alerts: List[AlertRecord]) -> int:
    """Batch insert alert records. Returns count saved."""

def get_alerts(
    session_id: Optional[str] = None,
    priority: Optional[str] = None,
    resolved: Optional[bool] = False,
    limit: int = 100,
) -> List[AlertRecord]:
    """Fetch alerts with optional filters."""

def resolve_alert(alert_id: str) -> bool:
    """Mark alert as resolved."""

def get_alert_summary() -> Dict:
    """Count by priority for dashboard gauges."""
```

---

## 6. Database Schema — `riect_alerts` Table

New table to be added to `db.py` `init_db()`:

```sql
CREATE TABLE IF NOT EXISTS riect_alerts (
    alert_id        TEXT PRIMARY KEY,       -- UUID
    created_at      TEXT NOT NULL,          -- ISO8601 datetime (IST)
    session_id      TEXT,                   -- FK: file_references.session_id
    priority        TEXT NOT NULL,          -- P1 | P2 | P3 | P4
    kpi_type        TEXT NOT NULL,          -- SPSF | SELL_THRU | DOI | DAYS_OF_COVER | MBQ
    signal_type     TEXT NOT NULL,          -- ANOMALY | RISK | FORECAST_DEVIATION
    dimension       TEXT NOT NULL,          -- "store_id" | "article_id" | "sku_id"
    dimension_value TEXT NOT NULL,          -- e.g., "Store_12"
    kpi_value       REAL,                   -- computed KPI value
    threshold       REAL,                   -- breach threshold
    gap             REAL,                   -- kpi_value - threshold
    status          TEXT,                   -- CRITICAL | HIGH | MEDIUM | MONITORING
    exception_text  TEXT,                   -- human-readable exception description
    recommended_action TEXT,               -- action text
    action_owner    TEXT,                   -- role responsible
    response_timeline TEXT,                -- "Today" | "3 days" etc.
    expected_impact TEXT,                  -- qualitative impact statement
    resolved        INTEGER DEFAULT 0,     -- 0=open, 1=resolved
    resolved_at     TEXT,                  -- ISO8601 when resolved
    FOREIGN KEY (session_id) REFERENCES file_references(session_id)
);

CREATE INDEX IF NOT EXISTS idx_riect_alerts_priority ON riect_alerts(priority);
CREATE INDEX IF NOT EXISTS idx_riect_alerts_session  ON riect_alerts(session_id);
CREATE INDEX IF NOT EXISTS idx_riect_alerts_resolved ON riect_alerts(resolved);
```

---

## 7. API Endpoints

### 7.1 `GET /api/alerts`

Returns ranked exception list (P1→P4).

**Query params:**
- `session_id` — filter by upload session (optional)
- `priority` — filter by P1/P2/P3/P4 (optional)
- `resolved` — `false` (default) | `true` | `all`
- `limit` — max results (default: 50)

**Response:**
```json
{
  "total": 7,
  "summary": {"P1": 2, "P2": 3, "P3": 2, "P4": 0},
  "alerts": [
    {
      "alert_id": "uuid-...",
      "priority": "P1",
      "kpi_type": "SPSF",
      "dimension_value": "Store_12",
      "kpi_value": 423,
      "exception_text": "Store 12 SPSF at 423 — 77 below P1 threshold (500)",
      "recommended_action": "Immediate floor re-plan or space reallocation",
      "action_owner": "Store Manager + Regional",
      "response_timeline": "Today",
      "expected_impact": "Recover SPSF toward 500 floor within 2 weeks",
      "created_at": "2026-02-20T14:30:00+05:30",
      "resolved": false
    }
  ]
}
```

---

### 7.2 `POST /api/alerts/run`

Trigger alert engine run on a specific uploaded session.

**Request:**
```json
{
  "session_id": "abc123",
  "min_priority": "P2"
}
```

**Response:**
```json
{
  "session_id": "abc123",
  "alerts_generated": 5,
  "summary": {"P1": 1, "P2": 2, "P3": 2},
  "run_at": "2026-02-20T14:35:00+05:30"
}
```

---

### 7.3 `GET /api/kpi/riect`

Returns computed RIECT KPI summary for a session.

**Query params:** `session_id`

**Response:**
```json
{
  "session_id": "abc123",
  "computed_at": "2026-02-20T14:30:00+05:30",
  "kpi_summary": {
    "spsf": {
      "avg": 847,
      "min": 423,
      "max": 1124,
      "target": 1000,
      "stores_below_target": 3,
      "stores_critical": 1
    },
    "sell_thru": {
      "avg_pct": 81.4,
      "target_pct": 95,
      "skus_critical": 2,
      "skus_high": 5,
      "markdown_triggers": 2
    },
    "doi": {
      "avg_days": 42,
      "max_days": 87,
      "overstock_count": 1,
      "reorder_trigger_count": 0
    }
  }
}
```

---

### 7.4 `PATCH /api/alerts/{alert_id}/resolve`

Mark alert as resolved.

**Response:** `{"alert_id": "...", "resolved": true, "resolved_at": "..."}`

---

## 8. Data Flow

```
1. User uploads data file → existing file engine processes → DataFrames ready

2. POST /api/alerts/run triggered (or auto-trigger on upload)
        ↓
3. kpi_controller.py merges DataFrames:
   sales_df + inventory_df + store_master + article_master + [git_df] + [forecast_df]
        ↓
4. Run all KPI engines in sequence:
   spsf_engine   → spsf_df (with spsf_status per store/period)
   sell_thru_engine → sell_thru_df (with sell_thru_status, markdown_trigger)
   doi_engine    → doi_df (with doi_status, overstock_flag, reorder_trigger)
   mbq_engine    → mbq_df (with mbq_compliant, so_trigger)
        ↓
5. alert_generator.py scans all KPI DataFrames:
   - Filter rows where status != "OK"
   - Apply priority_engine.py rules → classify P1/P2/P3
   - Apply compound rule (dual-P2 → P1 upgrade)
   - Build AlertRecord for each breach
        ↓
6. action_recommender.py → enriches each AlertRecord with action/owner/timeline
        ↓
7. alert_store.py → batch insert to riect_alerts table
        ↓
8. /api/alerts response → sorted P1→P4, with gap magnitude as secondary sort
        ↓
9. Frontend renders Alert Panel (above chat) with exception inbox
```

---

## 9. Frontend Integration

The Alert Panel renders from the `blocks` array in the WebSocket `done` message.
The Alert/Exception Engine will emit an `ALERT_PANEL` block type.

**Block emitted:**
```json
{
  "type": "alert_panel",
  "alerts": [
    {
      "rank": 1,
      "priority": "P1",
      "exception": "Store 12 SPSF at 423",
      "action": "Immediate floor re-plan",
      "timeline": "Today"
    }
  ],
  "summary": {"P1": 2, "P2": 3, "P3": 2}
}
```

**Existing frontend chart/KPI rendering** (app.js:1276) will be extended to handle `alert_panel` block type — displays the Exception Inbox table above the chat thread.

---

## 10. Integration with Existing `kpi/` Modules

The existing `kpi/` modules are **computation utilities** (not RIECT-aware).

| Existing Module | What It Does | RIECT Relationship |
|----------------|--------------|-------------------|
| `kpi/productivity.py` | Computes SPSF via `Sales_value / Store_SqFt` | `spsf_engine.py` wraps this with RIECT thresholds + breach detection |
| `kpi/store_kpi.py` | Computes Sell-Through %, Stock Cover | `sell_thru_engine.py` wraps with 95% target + status classification |
| `kpi/inventory_quality.py` | Dead stock %, age buckets | Referenced by `doi_engine.py` as supporting signal |
| `kpi/risk_scores.py` | MAPE-based forecast risk | Used by future Forecast Deviation signal engine |

**Principle:** RIECT engines **wrap** existing kpi/ functions — they do not duplicate them. The wrappers add: target thresholds, status classification, alert generation logic, and RIECT-specific column naming.

---

## 11. Graceful Degradation (Data Availability Matrix)

| Available Data | KPIs Computable | Alerts Generated |
|---------------|----------------|-----------------|
| Sales only | Sell-Thru (partial) | Sell-Thru alerts only |
| Sales + Inventory | Sell-Thru (full), DOI | Sell-Thru + DOI alerts |
| + Store Master (floor space) | + SPSF | + SPSF alerts |
| + Article Master + MBQ | + MBQ Compliance | + MBQ alerts |
| + GIT | DOI includes in-transit | More accurate DOI |
| + Forecast | Days of Cover | Stockout Risk alerts |

**No data = no crash.** Each engine checks column availability before computing. Missing data → warning logged + engine skipped for that KPI.

---

## 12. Build Order (Phase 1 Sprint)

| Step | Task | File | Depends On |
|------|------|------|-----------|
| 1 | Create `riect/__init__.py` | `backend/riect/__init__.py` | — |
| 2 | SPSF Engine | `riect/kpi_engine/spsf_engine.py` | `kpi/productivity.py` |
| 3 | Sell-Thru Engine | `riect/kpi_engine/sell_thru_engine.py` | `kpi/store_kpi.py` |
| 4 | DOI Engine | `riect/kpi_engine/doi_engine.py` | `kpi/inventory_quality.py` |
| 5 | MBQ Engine | `riect/kpi_engine/mbq_engine.py` | — |
| 6 | KPI Controller | `riect/kpi_engine/kpi_controller.py` | Steps 2–5 |
| 7 | Priority Engine | `riect/alert_engine/priority_engine.py` | — |
| 8 | Alert Generator | `riect/alert_engine/alert_generator.py` | Steps 6–7 |
| 9 | Action Recommender | `riect/alert_engine/action_recommender.py` | Step 7 |
| 10 | Alert Store | `riect/alert_engine/alert_store.py` | db.py (new table) |
| 11 | DB Migration | `db.py` — add `riect_alerts` table | — |
| 12 | Alerts API | `riect/api/alerts_api.py` | Steps 8–11 |
| 13 | KPI API | `riect/api/kpi_api.py` | Step 6 |
| 14 | Register in main.py | `main.py` router inclusion | Steps 12–13 |
| 15 | Frontend Alert Panel | `frontend/index.html` + `app.js` | Step 12 |
| 16 | Unit Tests | `backend/tests/test_riect_kpi.py` | Steps 2–13 |

---

## 13. Unit Test Plan

```
tests/test_riect_kpi.py:
  test_spsf_critical_below_500()
  test_spsf_ok_above_1000()
  test_sell_thru_p1_below_60()
  test_sell_thru_markdown_trigger()
  test_doi_overstock_above_90()
  test_doi_reorder_trigger_below_14()
  test_mbq_shortfall_detection()
  test_kpi_controller_with_missing_git()   # graceful degradation
  test_kpi_controller_with_missing_forecast()  # graceful degradation

tests/test_riect_alerts.py:
  test_alert_generation_from_kpi_breach()
  test_priority_p1_for_critical_spsf()
  test_compound_dual_p2_upgrade_to_p1()
  test_action_recommender_maps_all_priorities()
  test_alert_store_save_and_retrieve()
  test_api_alerts_returns_sorted_p1_first()
  test_api_alerts_run_end_to_end()
```

---

## 14. Configuration (Thresholds as Config, Not Hardcoded)

All thresholds to be configurable via `config.py` (or future RIECT config JSON):

```python
# In config.py — RIECT thresholds
RIECT_SPSF_TARGET = 1000
RIECT_SPSF_P1_THRESHOLD = 500
RIECT_SPSF_P2_THRESHOLD = 750

RIECT_SELL_THRU_TARGET = 95.0
RIECT_SELL_THRU_P1_THRESHOLD = 60.0
RIECT_SELL_THRU_P2_THRESHOLD = 80.0
RIECT_SELL_THRU_MARKDOWN_WEEKS = 6

RIECT_DOI_P1_THRESHOLD = 90
RIECT_DOI_P2_THRESHOLD = 60
RIECT_DOI_REORDER_THRESHOLD = 14

RIECT_DOC_P1_THRESHOLD = 7
RIECT_DOC_P2_THRESHOLD = 14
```

This allows the system operator to tune thresholds per retail format without code changes.

---

*Saved: 2026-02-20 | DSR|KRAI → RIECT Alert Engine Architecture v1.0*
*Owner: Dinesh Srivastava*
