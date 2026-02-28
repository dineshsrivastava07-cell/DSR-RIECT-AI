# RIECT Control Tower — Frontend UI & Components
## DSR|KRAI — Phase 2 UI Reference

**Version:** 1.0
**Date:** 2026-02-21
**Status:** Reference — Build after Phase 1 backend engines complete
**Owner:** Dinesh Srivastava

---

## 1. Overall Layout — Dual Pane

```
┌─────────────────────────────────────────────────────────────────┐
│  DSR|KRAI — RIECT Control Tower          [Role: HQ] [Session ▼] │
├──────────────────────────────┬──────────────────────────────────┤
│                              │                                  │
│    LEFT PANE                 │    RIGHT PANE                    │
│    Command / Dashboard       │    Chat Drill-Down               │
│    (always visible)          │    (always available)            │
│                              │                                  │
│  [KPI Gauges]                │  [Chat thread]                   │
│  [Exception Inbox]           │  [File upload]                   │
│  [Store Health Map]          │  [AI response]                   │
│  [Trend Sparklines]          │                                  │
│                              │                                  │
└──────────────────────────────┴──────────────────────────────────┘
```

- **Left Pane** — Command view: KPI status, exceptions, store health, trends
- **Right Pane** — Existing chat (already built): drill-down on any exception, file upload, AI response with inline charts
- Both panes always visible simultaneously
- Click any element in left pane → auto-fires query in right pane chat

---

## 2. Component 1 — Top Bar

```
┌──────────────────────────────────────────────────────────────────┐
│ DSR|KRAI RIECT  │ [Data: Week 8, Feb 2026 ▼] │ [Role: HQ ▼]     │
│                 │ Last updated: Today 09:15 IST│ [🔔 5 alerts]   │
└──────────────────────────────────────────────────────────────────┘
```

**Elements:**
- **Brand** — DSR|KRAI RIECT logo/title
- **Session/Period Selector** — dropdown to select active data upload (Week/Month/Season)
- **Last Updated** — timestamp of last data ingestion (IST)
- **Role Selector** — Store Manager / Regional / HQ / Vendor (changes scope of all panels)
- **Alert Bell** — unresolved alert count badge, click to jump to Exception Inbox

---

## 3. Component 2 — KPI Gauges (Top of Left Pane)

Four always-visible tiles showing current vs target with progress bar:

```
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  SPSF           │ │  Sell-Thru      │ │  DOI            │ │  Relevance      │
│  847 / 1,000    │ │  81% / 95%      │ │  42 days        │ │  76 / 100       │
│  [████████░░]   │ │  [████████░░]   │ │  [██████░░░░]   │ │  [███████░░░]   │
│  ▲12% WoW       │ │  ▲3% WoW        │ │  ▼8 days WoW    │ │  Stable         │
└─────────────────┘ └─────────────────┘ └─────────────────┘ └─────────────────┘
```

**Each tile contains:**
- KPI name
- Current value vs target
- Progress bar (fill % of target)
- Week-on-Week change with direction arrow (▲ / ▼)
- Trend label (Recovering / Declining / Stable)

**Colour coding:**
| Colour | Meaning |
|--------|---------|
| Green | On target / OK |
| Amber | P3 — Medium (monitoring) |
| Orange | P2 — High (action within 3 days) |
| Red | P1 — Critical (action today) |

**Interaction:**
- Click any gauge tile → chat drill-down auto-fires that KPI query
- Example: Click SPSF tile → *"Give me SPSF breakdown by store — highlight which stores are below target"*

---

## 4. Component 3 — Exception Inbox (Core of Left Pane)

Ranked P1→P4 alert table — the heart of RIECT:

```
┌───┬──────────────────────────────┬──────┬────────────────────┬──────────┬──────────┐
│ # │ Exception                    │ Pri  │ Recommended Action │ Timeline │ Action   │
├───┼──────────────────────────────┼──────┼────────────────────┼──────────┼──────────┤
│ 1 │ Store 12 SPSF dropped to 423 │  P1  │ Floor re-plan      │ Today    │ [Resolve]│
│ 2 │ SKU A47 Sell-Thru 54% Wk 8  │  P1  │ Markdown 20%       │ Today    │ [Resolve]│
│ 3 │ Vendor B delivery +9 days    │  P2  │ Escalate + alt     │ 3 days   │ [Resolve]│
│ 4 │ Category X DOI = 87 days     │  P2  │ Halt reorder       │ 3 days   │ [Resolve]│
│ 5 │ Q3 forecast deviation +18%   │  P3  │ Reforecast Q3      │ This wk  │ [Resolve]│
└───┴──────────────────────────────┴──────┴────────────────────┴──────────┴──────────┘
```

**Columns:**
- Rank (#) — sorted P1 first, then by gap magnitude
- Exception — human-readable breach description
- Priority — P1 / P2 / P3 / P4 colour-coded badge
- Recommended Action — from action playbook
- Timeline — Today / Within 3 days / This week / Next review
- Action — `[Resolve]` button → marks done, logs timestamp

**Filters (above table):**
- All | P1 | P2 | P3 | Unresolved | Resolved

**Interaction:**
- Click any row → chat drill-down opens with that exception pre-loaded as query
- `[Resolve]` button → `PATCH /api/alerts/{id}/resolve` → row greyed out

**Feeds from:** `GET /api/alerts` endpoint

---

## 5. Component 4 — Store Health Heatmap

Grid of all stores colour-coded by composite health score (SPSF + Sell-Thru + DOI combined):

```
Zone: North
┌────┬────┬────┬────┐
│ S1 │ S2 │ S3 │ S4 │     🟢 Green  = All KPIs OK
│ 🟢 │ 🟡 │ 🔴 │ 🟢 │     🟡 Amber  = 1 KPI at P3
├────┼────┼────┼────┤     🟠 Orange = 1 KPI at P2
│ S5 │ S6 │ S7 │ S8 │     🔴 Red    = Any P1 breach
│ 🟢 │ 🟠 │ 🟢 │ 🟡 │
└────┴────┴────┴────┘
```

**Composite Health Score logic:**
- Any P1 breach → Red
- Any P2 breach (no P1) → Orange
- Any P3 breach (no P1/P2) → Amber
- All KPIs OK → Green

**View toggles:**
- Zone view / Region view / State view / City view
- Hierarchy: Zone → Region → State → City → Location → Store

**Interaction:**
- Click any store cell → chat drill-down fires full KPI breakdown for that store
- Hover → tooltip with SPSF / Sell-Thru / DOI values for that store

---

## 6. Component 5 — Trend Sparklines

Mini trend lines for each KPI over last 4–8 weeks — shows trajectory direction:

```
SPSF trend:      ──────/▲   (recovering — positive)
Sell-Thru trend: ────\──    (declining — flag)
DOI trend:       ──────\▼   (reducing — positive for DOI)
```

**Displayed:**
- Inline within each KPI gauge tile (small, 8-week line)
- Also shown per store in Store Health Heatmap tooltip

**Visual indicators:**
- Arrow up (▲) + green = improving toward target
- Arrow down (▼) + context-dependent = improving (DOI) or declining (SPSF/Sell-Thru)
- Flat line = stable / no movement

---

## 7. Component 6 — Right Pane: Chat Drill-Down

The **existing DSR|KRAI chat** — right pane, unchanged.

**How it connects to Control Tower:**
- Click any Exception Inbox row → pre-populates query in chat
- Click any KPI gauge → pre-populates KPI-specific query
- Click any store cell → pre-populates store-specific query

**Example auto-queries triggered:**
| Left Pane Action | Chat Query Auto-fired |
|-----------------|----------------------|
| Click Store 12 (Red) | *"Analyse all KPIs for Store 12 — SPSF, Sell-Thru, DOI. What is driving the P1 breach and what actions are recommended?"* |
| Click SPSF gauge | *"Give me SPSF breakdown by store — which stores are below 1,000 target and by how much?"* |
| Click Exception Row 2 | *"SKU A47 Sell-Thru is at 54% in Week 8 — analyse root cause and give markdown recommendation"* |
| Click DOI gauge | *"Show DOI for all categories — which categories are above 60 days and what should we do?"* |

**Existing capabilities (already built, no change needed):**
- WebSocket streaming chat
- Inline charts (Chart.js)
- KPI blocks, Table blocks, Reasoning blocks
- File upload and data analysis
- Session persistence

---

## 8. Component 7 — Role-Based Views

Header role selector changes scope of all left pane components:

| Role | KPI Gauge Scope | Exception Inbox | Store Heatmap |
|------|----------------|----------------|--------------|
| **Store Manager** | Their store only | Their store alerts only | Their store only |
| **Regional Manager** | All stores in their region | Region alerts | Region heatmap |
| **HQ / Buyer** | All stores + all categories | Full exception inbox | All stores |
| **Vendor** | Delivery + GR metrics only | Vendor delivery alerts | Not shown |

**Implementation:** Role passed as query param to all API calls. Backend filters accordingly.

---

## 9. Frontend Technology Stack

| Layer | Technology | Notes |
|-------|-----------|-------|
| HTML structure | HTML5 | Extend existing `frontend/index.html` |
| Styling | CSS3 / existing styles | Match current DSR|KRAI theme |
| Charts/gauges | Chart.js | Already loaded (app.js:1276) |
| Progress bars | CSS + JS | Native — no extra library |
| Heatmap grid | CSS Grid + JS | Native — no extra library |
| Sparklines | Chart.js mini line charts | Reuse existing Chart.js |
| WebSocket | Existing WS client | No change to chat connection |
| API calls | `fetch()` REST calls | New — for /api/alerts, /api/kpi/riect |

---

## 10. New Block Type — `alert_panel`

The Alert/Exception Engine emits an `alert_panel` block in the WebSocket `done` message
alongside existing `chart`, `kpi`, `table`, `text` blocks.

**Block structure:**
```json
{
  "type": "alert_panel",
  "summary": {
    "P1": 2,
    "P2": 3,
    "P3": 2,
    "total_unresolved": 7
  },
  "alerts": [
    {
      "rank": 1,
      "priority": "P1",
      "kpi_type": "SPSF",
      "dimension_value": "Store_12",
      "kpi_value": 423,
      "exception": "Store 12 SPSF at 423 — 77 below P1 threshold",
      "action": "Immediate floor re-plan or space reallocation",
      "owner": "Store Manager + Regional",
      "timeline": "Today",
      "alert_id": "uuid-..."
    }
  ]
}
```

**Frontend renders** this block as the Exception Inbox table above the chat thread
(when returned as part of a chat response on data upload).

---

## 11. Page Load Sequence

```
1. Page loads → fetch GET /api/alerts → render Exception Inbox
2. Page loads → fetch GET /api/kpi/riect → render KPI Gauges
3. User selects role → re-fetch both APIs with role filter
4. User selects session/period → re-fetch both APIs with session_id filter
5. User uploads new data file → existing pipeline runs
                              → POST /api/alerts/run triggered automatically
                              → Exception Inbox refreshes
                              → KPI Gauges refresh
6. User clicks left pane element → query pre-populated in chat → WS sends → response streams
7. User clicks [Resolve] → PATCH /api/alerts/{id}/resolve → row updates inline
```

---

## 12. Build Order (Phase 2 — After Phase 1 Backend Complete)

| Step | Task | File |
|------|------|------|
| 1 | Add dual-pane layout to index.html | `frontend/index.html` |
| 2 | Add Top Bar (role selector, session dropdown, alert bell) | `frontend/index.html` + `app.js` |
| 3 | Add KPI Gauge tiles (4 tiles with progress bars) | `frontend/index.html` + `app.js` |
| 4 | Add Exception Inbox table with filter tabs | `frontend/index.html` + `app.js` |
| 5 | Wire Exception Inbox to `GET /api/alerts` | `app.js` |
| 6 | Wire KPI Gauges to `GET /api/kpi/riect` | `app.js` |
| 7 | Add Store Health Heatmap (CSS grid + colour logic) | `frontend/index.html` + `app.js` |
| 8 | Add Trend Sparklines (Chart.js mini lines) | `app.js` |
| 9 | Wire click interactions (left pane → chat query) | `app.js` |
| 10 | Wire `[Resolve]` button to `PATCH /api/alerts/{id}/resolve` | `app.js` |
| 11 | Render `alert_panel` block type from WS done message | `app.js` |
| 12 | Role-based view filtering (pass role to all API calls) | `app.js` |
| 13 | Auto-refresh after file upload (re-fetch alerts + KPIs) | `app.js` |

---

## 13. RIECT Docs Index

| Document | Contents |
|----------|---------|
| `RIECT-PLAN.md` | Vision, KPIs, 4-phase roadmap |
| `RIECT-DATA-INTEGRATION.md` | Full supply chain data model |
| `RIECT-ALERT-ENGINE-ARCHITECTURE.md` | Alert/KPI engine technical architecture |
| `RIECT-BACKEND-ENGINE.md` | Engines, data points, formulas, build order |
| `RIECT-CONTROL-TOWER-UI.md` | This document — Frontend UI & components |

---

*Saved: 2026-02-21 | DSR|KRAI → RIECT Control Tower UI Reference v1.0*
*Owner: Dinesh Srivastava*
