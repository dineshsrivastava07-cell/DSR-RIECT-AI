# RIECT — Retail Intelligence Execution Control Tower
## DSR|KRAI Strategic Enhancement Plan

**Version:** 1.0
**Date:** 2026-02-20
**Status:** Approved for Development

---

## 1. Vision

Transform DSR|KRAI from a reactive analytics chatbot into a **proactive, unified Retail Intelligence Execution Control Tower** — a single platform where every retail signal is seen, every risk is known, every decision is grounded in evidence, and every action is prioritised and tracked to execution.

> *"From data to decision in one platform. See every exception. Know every risk. Act with confidence."*

---

## 2. Business Motive & KPI Targets

RIECT exists to drive four measurable retail outcomes:

| KPI | Direction | Target | Why It Matters |
|---|---|---|---|
| **SPSF** (Sales Per Square Foot) | Increase | **1,000** | Measures space productivity — core profitability driver |
| **Sell-Thru %** | Increase | **95%** | Measures how much of bought stock is sold — reduces waste |
| **DOI** (Days of Inventory) | Reduce | Minimise | Excess inventory = capital locked, markdowns, waste |
| **Relevance** | Maintain | Sustained | Right product, right place, right time — assortment health |

RIECT achieves these targets by surfacing **anomaly detections, forecast deviations, and risk signals** early — with **prioritised, time-bound action points and recommendations** so execution happens before problems compound.

---

## 3. Core Operating Principle

```
SEE  →  KNOW  →  DECIDE  →  ACT  →  TRACK
```

| Stage | RIECT Capability | Output |
|---|---|---|
| **SEE** | Real-time visibility across stores, vendors, SKUs | Control Tower Dashboard |
| **KNOW** | Anomaly detection, forecast deviation, risk signals | Exception Inbox (ranked) |
| **DECIDE** | AI recommendations with business reasoning | Prioritised Action Points |
| **ACT** | One-click escalation, playbook execution | Execution Triggers |
| **TRACK** | Action logged → outcome measured next cycle | Closed-Loop Intelligence |

---

## 4. Signal Intelligence Framework

RIECT detects and prioritises three signal types:

### 4.1 Anomaly Detection
- Revenue drop > threshold vs prior period or plan
- SPSF below floor by store / category / floor area
- Sell-Thru below 80% with > X weeks on floor → markdown trigger
- DOI spike > Y days for a SKU/category → overstock alert
- Vendor delivery variance > Z days → supply risk

### 4.2 Forecast Deviation
- Actuals vs AOP/Rolling forecast variance flagged weekly
- Forward DOI projection — will stock last / will it overstock?
- Sell-Thru trajectory — on track for 95% by end of season?
- SPSF trend — accelerating or decelerating toward 1,000 target?

### 4.3 Risk Signals
- SKU stockout risk within N days (DOI < reorder threshold)
- Relevance decay — slow movers with low Sell-Thru + low SPSF
- Vendor concentration risk — single supplier for high-velocity SKUs
- Seasonal mis-buy — forward weeks of cover vs forecast demand

---

## 5. Action Point Framework

Every signal generates a **prioritised action point** with:

```
SIGNAL → ROOT CAUSE → RECOMMENDED ACTION → OWNER → TIMELINE → EXPECTED IMPACT
```

### Priority Tiers

| Priority | Trigger | Response Time | Example |
|---|---|---|---|
| **P1 — Critical** | SPSF < 500 or Sell-Thru < 60% or DOI > 90 days | Same day | Immediate markdown / transfer |
| **P2 — High** | SPSF 500–750 or Sell-Thru 60–80% or DOI 60–90 days | Within 3 days | Promotional push / reorder review |
| **P3 — Medium** | Forecast deviation > 15% | Within 1 week | Reforecast / assortment adjustment |
| **P4 — Monitor** | Trend deteriorating but within range | Next review cycle | Watch list |

---

## 6. Development Roadmap

### Phase 1 — Foundation *(Current Sprint)*
**Goal:** Make the existing analytics proactive and persistent

| Task | Status | Impact |
|---|---|---|
| Session persistence (chat_history DB write) | ✅ Done | All conversations saved |
| Inline auto-charts (BlockType fix) | ✅ Done | KPI + charts render in chat |
| llm_router local model availability fix | ✅ Done | Accurate model status |
| **Alert / Exception Engine** | 🔲 Next | Core RIECT capability |
| **SPSF / Sell-Thru / DOI KPI detection** | 🔲 Next | Business KPI grounding |

### Phase 2 — Control Tower Dashboard
**Goal:** Dual-pane UI — Command view + Chat drill-down

- Control Tower overview panel (KPI tiles, alerts, store health)
- Exception Inbox — ranked P1→P4 signals
- SPSF / Sell-Thru / DOI gauges with target indicators (1000 / 95% / min)
- Role-based views: Store Manager / Regional / HQ / Vendor
- Chat pane remains for drill-down on any exception

### Phase 3 — Real-Time Signal Ingestion
**Goal:** Replace manual upload with continuous data flow

- Watched folder / scheduled file pull
- Direct DB/ERP connector (CSV scheduled pull → auto-ingest)
- Multi-source merge: POS + Inventory + Vendor + Plan
- Auto-run full analytics pipeline on new data arrival
- Alert engine fires immediately on new data

### Phase 4 — Execution Loop
**Goal:** Close the loop — from signal to action to outcome

- Action recommendation engine with one-click escalation
- Playbook library — predefined responses for known exceptions
- Escalation paths: email / Slack / Teams notification
- Outcome tracking: log action → measure impact at next cycle
- Closed-loop SPSF / Sell-Thru / DOI improvement tracking

---

## 7. RIECT KPI Dashboard Spec

### Primary Gauges (always visible)
```
┌─────────────────────────────────────────────────────┐
│  SPSF: 847 / 1,000 ▲12%    [████████░░] 84.7%      │
│  Sell-Thru: 81% / 95% ▲3%  [████████░░] 85.3%      │
│  DOI: 42 days ▼8 days      [██████░░░░] Target: Min │
│  Relevance Score: 76/100   [███████░░░] Stable      │
└─────────────────────────────────────────────────────┘
```

### Exception Inbox (ranked by priority + impact)
```
┌───┬──────────────────────────────┬──────┬──────────────────┬──────────┐
│ # │ Exception                    │ Pri  │ Recommended Action│ Timeline │
├───┼──────────────────────────────┼──────┼──────────────────┼──────────┤
│ 1 │ Store 12 SPSF dropped to 423 │  P1  │ Floor re-plan     │ Today    │
│ 2 │ SKU A47 Sell-Thru 54%, Wk 8 │  P1  │ Markdown 20%      │ Today    │
│ 3 │ Vendor B delivery +9 days    │  P2  │ Escalate + alt    │ 3 days   │
│ 4 │ Category X DOI = 87 days     │  P2  │ Halt reorder      │ 3 days   │
│ 5 │ Q3 forecast deviation +18%   │  P3  │ Reforecast Q3     │ This week│
└───┴──────────────────────────────┴──────┴──────────────────┴──────────┘
```

---

## 8. Technical Architecture (Target State)

```
Data Sources (POS, ERP, Vendor, Plan)
        ↓
  Ingestion Layer (File Watch / Scheduled Pull / API)
        ↓
  Analytics Pipeline (Python-first, Evidence-Grounded)
        ↓
  Signal Engine (Anomaly | Deviation | Risk)
        ↓
  Priority Engine (P1 → P4, SPSF / Sell-Thru / DOI / Relevance)
        ↓
  Action Recommendation Engine (DIL + Playbooks)
        ↓
┌───────────────────────────────────────────┐
│         RIECT Control Tower UI            │
│  [Dashboard] [Exceptions] [Chat] [Track]  │
└───────────────────────────────────────────┘
        ↓
  Execution Triggers (Email / Slack / Teams)
        ↓
  Outcome Tracking (Closed-Loop)
```

---

## 9. Immediate Next Step

**Build: Alert / Exception Engine (Phase 1 completion)**

- Detect SPSF, Sell-Thru, DOI breaches from uploaded data
- Generate P1–P4 ranked exception list
- Expose via `/api/alerts` endpoint
- Show in UI as persistent alert panel above chat

This is the single highest-impact next build — it is the foundation of RIECT.

---

*Saved: 2026-02-20 | DSR|KRAI → RIECT Enhancement Plan v1.0*
*Owner: Dinesh Srivastava*
