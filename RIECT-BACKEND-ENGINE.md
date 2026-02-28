# RIECT Backend Engine — Reference Document
## DSR|KRAI — Engines, Data Points & KPI Formulas

**Version:** 1.0
**Date:** 2026-02-21
**Status:** Reference — Build when ready
**Owner:** Dinesh Srivastava

---

## 1. Backend Engines

### Engine 1 — KPI Detection Engine

Location: `backend/riect/kpi_engine/`

| Engine | What It Computes | Output |
|--------|-----------------|--------|
| `spsf_engine` | Sales ÷ Floor Space per store/period | SPSF value, gap to 1000, P1/P2/P3 status |
| `sell_thru_engine` | Net Sales Qty ÷ (Sales + Inventory) | Sell-Thru %, markdown trigger flag |
| `doi_engine` | Stock ÷ Avg Daily Sales (4-week) | DOI days, overstock flag, reorder trigger |
| `doi_engine` | Stock ÷ Forecasted Daily Rate | Days of Cover, stockout risk |
| `mbq_engine` | Store Inventory vs MBQ floor | Shortfall qty, SO trigger flag |
| `kpi_controller` | Orchestrates all above | Single unified KPI result set |

---

### Engine 2 — Alert / Exception Engine

Location: `backend/riect/alert_engine/`

| Engine | What It Does | Output |
|--------|-------------|--------|
| `priority_engine` | Applies P1–P4 rules to each KPI breach | Priority level per alert |
| `alert_generator` | Scans KPI output → creates alert records | List of AlertRecord objects |
| `action_recommender` | Maps alert type + priority → action playbook | Action, Owner, Timeline, Impact |
| `alert_store` | Reads/writes `riect_alerts` DB table | Persistent alert history |

---

### Engine 3 — API Layer

Location: `backend/riect/api/`

| Endpoint | Purpose |
|----------|---------|
| `GET /api/alerts` | Ranked P1→P4 exception list |
| `POST /api/alerts/run` | Trigger engine on uploaded session |
| `GET /api/kpi/riect` | Full KPI summary for dashboard gauges |
| `PATCH /api/alerts/{id}/resolve` | Mark alert as resolved |

---

## 2. Data Points Required

### Master Data (Reference — loaded once)

| Entity | Key Fields |
|--------|-----------|
| **Article Master** | article_id, option_id, sku_id, division, section, department, price_tier (Eco/Regular/Popular/Premium), mrp |
| **Store Master** | store_id, store_name, city, state, region, zone, floor_space_sqft |
| **Article MBQ by Store** | article_id, store_id, mbq_qty (minimum floor units) |

---

### Transactional Data (Uploaded per period)

| Entity | Key Fields | Feeds Engine |
|--------|-----------|-------------|
| **Sales Data** | article_id, option_id, sku_id, store_id, date, gross_sales_qty, gross_sales_amount, discount_pct, discount_amount, promo_pct, promo_amount, net_sales_qty, net_sales_amount | SPSF, Sell-Thru, DOI |
| **Inventory (Store SOH)** | article_id, store_id, date, inventory_qty, inventory_amount | Sell-Thru, DOI, MBQ |
| **Goods Receipts (Warehouse)** | po_id, article_id, gr_date, gr_qty, gr_amount | Supply signal |
| **Goods in Transit (GIT)** | so_id, article_id, store_id, dispatch_date, expected_arrival, git_qty | DOI (numerator) |
| **Stock Out (SO)** | so_id, article_id, store_id, so_date, so_qty, so_status, pick_deadline, units_picked | MBQ, replenishment |
| **Pending SO** | so_id where so_status='Open' AND pick_deadline < today AND units_picked=0 | Supply risk signal |

---

### Planning Data

| Entity | Key Fields | Feeds Engine |
|--------|-----------|-------------|
| **Buying Plan** | article_id, season_code, planned_buy_qty, planned_buy_amount, delivery_date | Forecast deviation |
| **Purchase Order (PO)** | po_id, vendor_id, article_id, po_date, po_qty, po_amount, expected_delivery_date, actual_delivery_date | Vendor risk signal |
| **Plan / AOP** | article_id, store_id, period, planned_net_sales_qty, planned_net_sales_amount, planned_spsf, planned_sell_thru_pct | KPI vs plan variance |
| **Forecast Data** | article_id, store_id, period, forecast_daily_sales_rate | Days of Cover engine |
| **Warehouse Stock** | article_id, date, warehouse_qty, warehouse_amount | Supply availability |

---

### Pending Data Entities (Not yet defined — needed for full accuracy)

| Entity | Priority | Unlocks |
|--------|----------|---------|
| **Markdown / Price Event** | HIGH (P1) | Sell-Thru trend accuracy, markdown trigger validation |
| **Trading / Season Calendar** | HIGH (P2) | Weeks-on-floor for markdown trigger, season-end Sell-Thru trajectory |
| **Open-to-Buy (OTB)** | MEDIUM (P3) | Forward buying constraint signal |
| **Inter-Store Transfer** | MEDIUM (P4) | Stock rebalancing recommendation |
| **Floor Space Plan × Price Tier** | MEDIUM (P5) | SPSF by tier/range plan |
| **Lost Sales / Stockout Log** | LOW (P6) | Missed revenue signal |
| **Replenishment History** | LOW (P7) | Pattern-based reorder intelligence |

---

## 3. KPI Formulas

```
SPSF            = Net Sales Amount ÷ Floor Space (sq ft)
                  Target: ≥ 1,000

Sell-Thru %     = Net Sales Qty ÷ (Net Sales Qty + Inventory Qty) × 100
                  Target: ≥ 95%

DOI (backward)  = (Store Inventory Qty + GIT Qty) ÷ Avg Daily Net Sales (4-week rolling)
                  Target: Minimise

Days of Cover   = (Store Inventory Qty + GIT Qty) ÷ Forecasted Daily Sales Rate
                  Target: Minimise

MBQ Compliance  = Store Inventory Qty ≥ Article MBQ (by store)
                  Target: 100% compliant
```

---

## 4. Priority Thresholds

### SPSF
| Value | Priority | Status |
|-------|----------|--------|
| < 500 | P1 | CRITICAL — immediate floor re-plan |
| 500–750 | P2 | HIGH — promotional push |
| 750–1000 | P3 | MEDIUM — monitor trajectory |
| ≥ 1000 | OK | On target |

### Sell-Thru %
| Value | Priority | Status |
|-------|----------|--------|
| < 60% | P1 | CRITICAL — immediate markdown |
| 60–80% | P2 | HIGH — promotional push |
| 80–95% | P3 | MEDIUM — monitor trajectory |
| ≥ 95% | OK | On target |

### DOI (Days of Inventory)
| Value | Priority | Status |
|-------|----------|--------|
| > 90 days | P1 | CRITICAL — halt reorder, initiate clearance |
| 60–90 days | P2 | HIGH — halt reorder review |
| 30–60 days | P3 | MEDIUM — watch |
| ≤ 30 days | OK | Acceptable |

### Days of Cover (forward)
| Value | Priority | Status |
|-------|----------|--------|
| < 7 days | P1 | CRITICAL — emergency replenishment |
| 7–14 days | P2 | HIGH — raise SO urgently |
| 14–21 days | P3 | MEDIUM — monitor |
| ≥ 21 days | OK | Adequate cover |

---

## 5. What Already Exists in DSR|KRAI (Do Not Rebuild)

| Existing Module | What It Has | RIECT Usage |
|----------------|------------|-------------|
| `kpi/productivity.py` | Basic SPSF (Sales ÷ SqFt) | `spsf_engine` wraps with thresholds + alert logic |
| `kpi/store_kpi.py` | Sell-Through %, Stock Cover | `sell_thru_engine` wraps with 95% target + status |
| `kpi/inventory_quality.py` | Dead stock %, age buckets | Referenced by `doi_engine` as supporting signal |
| `kpi/risk_scores.py` | Forecast risk (MAPE-based) | Used by future Forecast Deviation engine |
| `analytics/` | Store aggregation, geo rollup | Feed data into KPI controller |

> **Principle:** RIECT engines **wrap** existing kpi/ functions — add thresholds, status classification, and alert generation on top. No duplication.

---

## 6. Supply Chain Flow (Context)

```
Buying Plan → Purchase Order (PO)
                    ↓
           Good Receipt (GR) at Warehouse only
                    ↓
           Warehouse Stock
                    ↓
           Rule Engine (MBQ check per store)
                    ↓
           Stock Out (SO) raised
                    ↓
        ┌───────────────────────┐
        │                       │
   Picked → GIT (in transit)   Not Picked → Pending SO
        │
   Delivered → Store Inventory (SOH)
```

All stores fed by warehouse only. No direct vendor-to-store delivery.

---

## 7. Build Order (When Ready)

| Step | File | Depends On |
|------|------|-----------|
| 1 | `backend/riect/__init__.py` | — |
| 2 | `riect/kpi_engine/spsf_engine.py` | `kpi/productivity.py` |
| 3 | `riect/kpi_engine/sell_thru_engine.py` | `kpi/store_kpi.py` |
| 4 | `riect/kpi_engine/doi_engine.py` | `kpi/inventory_quality.py` |
| 5 | `riect/kpi_engine/mbq_engine.py` | — |
| 6 | `riect/kpi_engine/kpi_controller.py` | Steps 2–5 |
| 7 | `riect/alert_engine/priority_engine.py` | — |
| 8 | `riect/alert_engine/alert_generator.py` | Steps 6–7 |
| 9 | `riect/alert_engine/action_recommender.py` | Step 7 |
| 10 | `riect/alert_engine/alert_store.py` | db.py (new table) |
| 11 | `db.py` — add `riect_alerts` table | — |
| 12 | `riect/api/alerts_api.py` | Steps 8–11 |
| 13 | `riect/api/kpi_api.py` | Step 6 |
| 14 | Register routers in `main.py` | Steps 12–13 |
| 15 | Frontend Alert Panel | Step 12 |
| 16 | Unit Tests | Steps 2–13 |

---

*Saved: 2026-02-21 | DSR|KRAI → RIECT Backend Engine Reference v1.0*
*Owner: Dinesh Srivastava*
