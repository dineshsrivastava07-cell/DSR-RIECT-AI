# RIECT — Required Data Points for Integration
## Reference Document for Data Integration Planning

**Version:** 1.3
**Date:** 2026-02-20
**Status:** Reference — Integration Planning Phase

---

## 1. KPI Formula Reference

| KPI | Formula | Target |
|---|---|---|
| **SPSF** (Sales Per Square Foot) | Net Sales ÷ Selling Floor Area (sq ft) | **1,000** |
| **Sell-Thru %** | Units Sold ÷ Units Received at Store × 100 | **95%** |
| **DOI** (Days of Inventory) | (Store SOH + GIT) ÷ Avg Daily Sales | **Minimise** |
| **Days of Cover** | (Store SOH + GIT) ÷ Forecasted Daily Sales Rate | **Right-size per category** |
| **MBQ Compliance** | Store SOH ≥ Article MBQ → Compliant | **100% compliance** |
| **Relevance** | Sell-Thru of new arrivals + Margin health + Stock age | **Maintain** |

> **DOI vs Days of Cover:**
> - **DOI** uses *actual* avg daily sales (backward-looking) — how long current stock will last at historic rate
> - **Days of Cover** uses *forecasted* daily sales rate (forward-looking) — how long stock covers future demand
> - Both use Store SOH + GIT as the numerator (full pipeline stock)
>
> **MBQ (Minimum Base Quantity):** Article-level minimum units a store must hold on floor at all times.
> If Store SOH < Article MBQ → triggers SO via Rule Engine for replenishment.

---

## 2. Supply Chain Architecture (Warehouse-Centric)

> **All stores are fed by warehouse only.**
> **All Purchase Order receipts are Good Receipts into the warehouse only.**
> **No vendor delivers directly to store.**

```
BUYING PLAN
    ↓
PURCHASE ORDER (PO) ──→ Vendor Ships
                              ↓
                    GOOD RECEIPT (GR) ← into Warehouse only
                              ↓
                    WAREHOUSE STOCK
                              ↓
                    RULE ENGINE (Allocation Logic)
                              ↓
                    SALES ORDER (SO) generated per store
                         /           \
                    Picked?         Not Picked
                       ↓                ↓
               GOODS IN TRANSIT    PENDING SO
               (WH → Store)        (Backlog Alert)
                       ↓
               STORE RECEIPT (GR at store)
                       ↓
               STORE STOCK (On-Floor Inventory)
                       ↓
               SALES (POS)
```

---

## 3. Core Data Entities (13 Tables)

---

### 3.1 Sales Data *(Daily, by Store + SKU)*
**Source:** POS / ERP
**Frequency:** Daily
**Granularity:** Every row is at Article / Option / SKU level — Discount % / Amount and Promo % / Amount captured per row item

| Field | Type | Used For |
|---|---|---|
| `date` | Date | Trending, DOI calc |
| `store_id` | String | Store-level SPSF |
| `sku_id` | String | SKU-level Sell-Thru, DOI |
| `category` | String | Category analysis |
| `net_sales_qty` | Integer | **Net Sales Qty** — units sold net of returns (Sell-Thru, DOI velocity) |
| `gross_sales_amount` | Decimal | Gross Sales Amount before any discount or promo |
| `discount_pct` | Decimal | **Discount %** — regular discount applied (e.g. clearance, loyalty, employee) |
| `discount_amount` | Decimal | **Discount Amount** — value of discount applied |
| `promo_pct` | Decimal | **Promo %** — promotional offer applied (e.g. campaign, weekend sale, BOGO) |
| `promo_amount` | Decimal | **Promo Amount** — value of promotional offer applied |
| `net_sales_amount` | Decimal | **Net Sales Amount** = Gross Sales − Discount − Promo (SPSF numerator) |
| `gross_margin_amount` | Decimal | Gross margin in value after discount and promo |
| `gross_margin_pct` | Decimal | Gross margin % after discount and promo |

> **Net Sales Amount formula:**
> `Net Sales Amount = Gross Sales Amount − Discount Amount − Promo Amount`
>
> **Discount vs Promo:**
> - **Discount** — regular price reduction: clearance markdown, loyalty card, employee, negotiated
> - **Promo** — campaign-driven offer: weekend sale, festive promo, BOGO, bundle, category event
> - Both erode margin — tracked separately to isolate organic margin vs promo-driven sell-thru

---

### 3.2 Store Inventory / Stock Data *(Daily snapshot, by Store + SKU)*
**Source:** WMS / ERP — Store level
**Frequency:** Daily snapshot

| Field | Type | Used For |
|---|---|---|
| `date` | Date | DOI calculation |
| `store_id` | String | Store DOI |
| `sku_id` | String | SKU DOI |
| `inventory_qty` | Integer | **Inventory Qty** — units on hand at store (DOI numerator) |
| `inventory_amount` | Decimal | **Inventory Amount** — stock value at cost |
| `weeks_on_floor` | Integer | Relevance (stock age) |
| `arrival_date` | Date | Relevance cohort |
| `reorder_point` | Integer | Stockout risk / SO trigger |

---

### 3.3 Buying Plan *(Season / Category / SKU level)*
**Source:** Merchandise planning system
**Frequency:** Per season / updated on replan

> The planned buy — what the business intends to purchase before POs are raised.
> Used to track buy vs actual PO vs actual receipt variance.

| Field | Type | Used For |
|---|---|---|
| `plan_id` | String | Unique plan reference |
| `season` | String | Season code |
| `category` | String | Category |
| `sub_category` | String | Sub-category |
| `sku_id` | String | SKU-level plan |
| `store_id` | String | Store allocation plan |
| `planned_units` | Integer | Planned buy quantity |
| `planned_cost` | Decimal | Planned cost value |
| `planned_retail` | Decimal | Planned retail value |
| `planned_margin_pct` | Decimal | Planned margin % |
| `planned_sell_thru_pct` | Decimal | Target Sell-Thru for this buy |
| `buy_date` | Date | When buy was finalised |

---

### 3.4 Purchase Order (PO) *(by Vendor + SKU)*
**Source:** Procurement / ERP
**Frequency:** Per PO raised

> Confirmed orders placed to vendors. Received only at warehouse (never direct to store).

| Field | Type | Used For |
|---|---|---|
| `po_number` | String | Unique PO reference |
| `po_date` | Date | Order placement date |
| `vendor_id` | String | Vendor linkage |
| `sku_id` | String | SKU ordered |
| `season` | String | Season code |
| `units_ordered` | Integer | PO quantity |
| `cost_price` | Decimal | Unit cost |
| `expected_delivery_date` | Date | Promised delivery to warehouse |
| `po_status` | String | Open / Partial / Closed / Cancelled |

---

### 3.5 Good Receipt (GR) — Warehouse *(PO receipt into warehouse)*
**Source:** WMS
**Frequency:** Per receipt event

> All PO receipts land here — warehouse only. Triggers warehouse stock update.

| Field | Type | Used For |
|---|---|---|
| `gr_number` | String | Unique GR reference |
| `gr_date` | Date | Date received at warehouse |
| `po_number` | String | Linked PO |
| `vendor_id` | String | Vendor |
| `sku_id` | String | SKU received |
| `gr_qty_gross` | Integer | Total units received from vendor (gross) |
| `gr_qty` | Integer | **GR Qty** — units accepted after QC (net, used for Sell-Thru denominator) |
| `gr_amount` | Decimal | **GR Amount** — accepted qty × cost price (inventory value added) |
| `gr_qty_rejected` | Integer | Units failed QC / returned to vendor |
| `warehouse_id` | String | Receiving warehouse |
| `gr_status` | String | Complete / Partial / Under Query |

---

### 3.6 Warehouse Stock *(Daily snapshot, by Warehouse + SKU)*
**Source:** WMS
**Frequency:** Daily snapshot

> Central stock position. Fed by GR. Depleted by SO dispatch.

| Field | Type | Used For |
|---|---|---|
| `date` | Date | Stock position date |
| `warehouse_id` | String | Warehouse |
| `sku_id` | String | SKU |
| `inventory_qty` | Integer | **Inventory Qty** — total units on hand at warehouse |
| `inventory_amount` | Decimal | **Inventory Amount** — stock value at cost |
| `reserved_qty` | Integer | Units reserved for open SOs (not yet dispatched) |
| `available_qty` | Integer | Inventory Qty minus Reserved Qty (available to allocate) |

---

### 3.7 Sales Order (SO) — Store Replenishment *(Rule Engine Output)*
**Source:** Rule Engine / WMS / ERP
**Frequency:** Per allocation run (daily / triggered)

> Generated by the Rule Engine allocation logic to replenish stores from warehouse.
> SO = instruction for warehouse to pick and dispatch to store.
> If not picked → becomes Pending SO.

| Field | Type | Used For |
|---|---|---|
| `so_number` | String | Unique SO reference |
| `so_date` | Date | SO creation date (rule engine run date) |
| `store_id` | String | Destination store |
| `warehouse_id` | String | Source warehouse |
| `sku_id` | String | SKU to dispatch |
| `units_allocated` | Integer | Units the rule engine allocated |
| `units_picked` | Integer | Units actually picked (0 if pending) |
| `units_dispatched` | Integer | Units dispatched to store |
| `so_status` | String | **Open / Pending / Picked / Dispatched / Received / Cancelled** |
| `required_by_date` | Date | Store needs stock by this date |
| `pick_deadline` | Date | Warehouse must pick by this date |
| `dispatch_date` | Date | Actual dispatch date (null if pending) |

> **Pending SO Rule:** `so_status = 'Open'` AND `pick_deadline < today` AND `units_picked = 0`

---

### 3.8 Goods in Transit (GIT) *(Dispatched WH → Store, not yet received)*
**Source:** WMS / Logistics / TMS
**Frequency:** Real-time / Daily update

> Stock dispatched from warehouse, in transit to store.
> Included in DOI calculation as available future stock for the store.

| Field | Type | Used For |
|---|---|---|
| `git_id` | String | Unique GIT reference |
| `so_number` | String | Linked SO |
| `dispatch_date` | Date | Date left warehouse |
| `store_id` | String | Destination store |
| `warehouse_id` | String | Origin warehouse |
| `sku_id` | String | SKU in transit |
| `units_in_transit` | Integer | Units dispatched, not yet received |
| `expected_delivery_date` | Date | Expected store receipt date |
| `actual_delivery_date` | Date | Actual store receipt date (null if in transit) |
| `transit_status` | String | In Transit / Delivered / Delayed |
| `days_in_transit` | Integer | Days since dispatch |

---

### 3.9 Store Master *(Static, updated on change)*
**Source:** Real estate / ops master file
**Frequency:** On store opening / change
**Hierarchy:** Zone > Region > State > City > Location > Store

| Field | Type | Used For |
|---|---|---|
| `store_id` | String | Unique store code — join key |
| `store_name` | String | Display name |
| `zone` | String | Broadest geographic grouping (e.g. North, South, East, West) |
| `region` | String | Within zone (e.g. North-1, North-2) |
| `state` | String | State / province |
| `city` | String | City |
| `location` | String | Location type (e.g. Mall / High Street / Standalone / Airport) |
| `location_name` | String | Specific location name (e.g. "Ambience Mall Gurgaon") |
| `selling_floor_sqft` | Decimal | **SPSF denominator — critical** (selling area only, not GLA) |
| `store_format` | String | Store format (e.g. Large / Medium / Small / Express) |
| `tier` | String | A / B / C store tier (performance classification) |
| `opening_date` | Date | Store opening date (maturity adjustment) |
| `warehouse_id` | String | Assigned feeder warehouse (all replenishment from here) |
| `is_active` | Boolean | Active / Closed |

---

### 3.10 Plan / AOP Data *(Weekly or Monthly targets)*
**Source:** Finance / Planning team
**Frequency:** Monthly (updated on reforecast)

| Field | Type | Used For |
|---|---|---|
| `week` / `month` | Date | Period alignment |
| `store_id` | String | Store-level plan |
| `category` | String | Category plan |
| `planned_sales_value` | Decimal | Planned Net Sales Amount (Actual Plan — no discount/promo breakdown) |
| `planned_units` | Integer | Planned volume |
| `planned_spsf` | Decimal | SPSF target |
| `planned_sell_thru` | Decimal | Sell-Thru target |
| `planned_doi` | Decimal | DOI target |
| `planned_margin` | Decimal | Planned Margin % |

> **Note:** Plan / AOP is an **Actual Plan** — discount and promo are not broken out separately at plan level. Discount and Promo % / Amount are captured only in **Sales Data** at the Article / Option / SKU row level.

---

### 3.11 Vendor / Supply Data *(Per vendor performance)*
**Source:** Vendor portal / ERP
**Frequency:** Per PO / shipment

| Field | Type | Used For |
|---|---|---|
| `vendor_id` | String | Vendor risk |
| `vendor_name` | String | Display |
| `po_number` | String | PO linkage |
| `promised_delivery_date` | Date | Lead time baseline |
| `actual_delivery_date` | Date | Delay detection |
| `units_ordered` | Integer | Supply plan |
| `units_delivered` | Integer | Fulfilment rate |
| `sku_id` | String | SKU supply risk |
| `lead_time_days` | Integer | Vendor lead time |

---

### 3.12 Product / Article Master *(Static reference)*
**Source:** Merchandise master / product catalogue / ERP
**Frequency:** On new product introduction / update
**Hierarchy:** Division > Section > Department > Article > Option > SKU

| Field | Type | Used For |
|---|---|---|
| **— Hierarchy —** | | |
| `division_id` | String | Top-level product division (e.g. Men / Women / Kids / Home) |
| `division_name` | String | Division display name |
| `section_id` | String | Within division (e.g. Apparel / Footwear / Accessories) |
| `section_name` | String | Section display name |
| `department_id` | String | Within section (e.g. Tops / Bottoms / Innerwear / Denim) |
| `department_name` | String | Department display name |
| `article_id` | String | Style / design level — unique style code |
| `article_name` | String | Article display name |
| `option_id` | String | Colour / variant of an article (Article + Colour) |
| `option_name` | String | Option display name (e.g. "Polo Shirt A — Navy Blue") |
| `sku_id` | String | Lowest level — Article + Option + Size — **join key** |
| `sku_name` | String | Full SKU display name |
| **— Attributes —** | | |
| `brand` | String | Brand |
| `season` | String | Season code (e.g. SS25 / AW25) |
| `gender` | String | Men / Women / Kids / Unisex |
| `size` | String | Size label (e.g. S / M / L / XL / 30 / 32) |
| `colour` | String | Colour name |
| `price_tier` | String | **Eco / Regular / Popular / Premium** — article price positioning |
| `mrp` | Decimal | **Maximum Retail Price** — legally mandated price printed on product (set per price_tier) |
| `cost_price` | Decimal | Standard cost |
| `gross_margin_pct` | Decimal | Target gross margin % (derived from MRP and cost) |
| `is_core` | Boolean | Core (always-on) vs Fashion (seasonal) |
| `lifecycle_stage` | String | New / Active / Clearance / EOL |
| **— Replenishment —** | | |
| `mbq` | Integer | **Minimum Base Quantity** — minimum units store must hold on floor |
| `mbq_unit` | String | Per store / per format / per tier (scope of MBQ) |
| `reorder_multiple` | Integer | Units must be replenished in multiples of this |

> **Price Tier Classification:**
> | Tier | Description | MRP Range (indicative) |
> |---|---|---|
> | **Eco** | Economy — entry price point, high volume | Lowest MRP band |
> | **Regular** | Standard — core everyday range | Mid-low MRP band |
> | **Popular** | Popular — best-seller sweet spot | Mid-high MRP band |
> | **Premium** | Premium — aspirational, higher margin | Highest MRP band |
>
> Price tier drives: MRP → Margin → Floor space allocation (MRP plan) → MBQ → SPSF contribution per article

> **MBQ (Minimum Base Quantity):**
> The minimum number of units of this Article that must be on the store floor at all times.
> - If Store SOH < MBQ → Rule Engine generates SO for replenishment
> - MBQ can be set at Article level (same for all stores) or Article × Store Tier level
> - MBQ breach = immediate replenishment trigger, not just a low-stock flag

---

### 3.13 Article MBQ by Store *(Replenishment Rule — Article × Store level)*
**Source:** Merchandise planning / replenishment rule engine config
**Frequency:** Per season / updated by planners

> Overrides the master MBQ when a store-specific minimum is required.
> If this table exists for an Article × Store combination, it takes precedence over 3.12 MBQ.

| Field | Type | Used For |
|---|---|---|
| `article_id` | String | Article reference |
| `store_id` | String | Store reference |
| `mbq` | Integer | Store-specific minimum base quantity |
| `days_of_cover_min` | Integer | Minimum days of cover required for this article at this store |
| `days_of_cover_max` | Integer | Maximum days of cover (beyond this = overstock signal) |
| `effective_from` | Date | Rule effective from |
| `effective_to` | Date | Rule effective to (null = open-ended) |

---

## 4. Signal-to-Data Mapping

| RIECT Signal | Data Entities Required |
|---|---|
| SPSF breach | Sales Data + Store Master (floor sqft + zone/region/city) |
| Sell-Thru at risk | Sales Data + GR (warehouse) + Store Inventory |
| DOI spike | Store Inventory + GIT + Sales Data (velocity) |
| **Days of Cover below min** | Store Inventory + GIT + Article MBQ by Store (days_of_cover_min) + Forecast |
| **Days of Cover above max** | Store Inventory + GIT + Article MBQ by Store (days_of_cover_max) → overstock |
| **MBQ breach** | Store Inventory + Article Master (MBQ) + Article MBQ by Store |
| Relevance decay | Store Inventory (weeks on floor) + Sales + Article Master (lifecycle_stage) |
| Forecast deviation | Sales Data + Plan/AOP Data |
| Stockout risk | Store Inventory + GIT + Pending SO + Sales velocity + MBQ |
| **Pending SO backlog** | Sales Order (so_status=Pending) + Store Inventory + MBQ |
| **GIT delay** | GIT (transit_status=Delayed) + expected vs actual delivery |
| Vendor delay | Vendor Data + PO + GR (expected vs actual GR date) |
| Markdown / Discount pressure | Sales (discount_pct + discount_amount) + Store Inventory (weeks on floor) + Article Master (MRP + price_tier) |
| Promo effectiveness | Sales (promo_pct + promo_amount + net_sales_qty) vs Plan/AOP (planned_promo_pct + planned_sell_thru) |
| Margin erosion | Sales (gross_margin_pct) vs Plan/AOP (planned_margin) — driven by discount + promo depth |
| Buy vs actual variance | Buying Plan + PO + GR (warehouse) |
| Warehouse stock risk | Warehouse Stock + open SOs + Pending SOs |
| **Zone / Region performance** | Sales + Store Master (zone/region/state/city) + Plan/AOP |
| **Division / Dept performance** | Sales + Article Master (division/section/department) + Plan/AOP |

---

## 5. Integration Priority

| Priority | Data Entity | Reason |
|---|---|---|
| **P1 — Must Have** | Sales Data | SPSF, Sell-Thru, DOI velocity |
| **P1 — Must Have** | Store Inventory / Stock | Store DOI, stockout, relevance |
| **P1 — Must Have** | Store Master | SPSF denominator (floor sqft) |
| **P1 — Must Have** | Good Receipt (GR) — Warehouse | Sell-Thru denominator (what actually arrived) |
| **P1 — Must Have** | Goods in Transit (GIT) | True DOI = store stock + GIT |
| **P1 — Must Have** | Sales Order (SO) + Pending SO | Replenishment health, backlog alerts |
| **P2 — High** | Buying Plan | Buy vs actual tracking, OTB |
| **P2 — High** | Purchase Order (PO) | PO vs GR variance, vendor lead time |
| **P2 — High** | Warehouse Stock | Allocation capacity, SO fulfilment |
| **P2 — High** | Plan / AOP Data | Forecast deviation signals |
| **P1 — Must Have** | Article Master (full hierarchy) | Division/Section/Dept/Article/Option/SKU rollups + MBQ |
| **P1 — Must Have** | Article MBQ by Store | Replenishment trigger, Days of Cover min/max |
| **P2 — High** | Store Master (full hierarchy) | Zone/Region/State/City/Location rollups + SPSF |
| **P3 — Medium** | Vendor / Supply Data | Vendor risk, delay signals |

---

## 6. Minimum Viable Dataset (MVP Integration)

**8 files unlock all KPIs + replenishment + hierarchy signals:**

```
1. sales_daily.csv           → SPSF + Sell-Thru velocity + DOI denominator
2. store_inventory.csv       → Store DOI + Days of Cover + Relevance + Stockout
3. store_master.csv          → Zone>Region>State>City>Location>Store + floor sqft
4. article_master.csv        → Division>Section>Dept>Article>Option>SKU + MBQ
5. article_mbq_by_store.csv  → Store-level MBQ + Days of Cover min/max rules
6. good_receipts_wh.csv      → Sell-Thru denominator (actual stock received)
7. goods_in_transit.csv      → True DOI/Days of Cover (store stock + pipeline)
8. sales_orders.csv          → Replenishment health + Pending SO + MBQ breach
```

Once MVP is validated, add:
```
9.  buying_plan.csv          → Buy vs actual variance, OTB tracking
10. purchase_orders.csv      → PO vs GR variance, lead time risk
11. warehouse_stock.csv      → Central stock position, allocation capacity
12. vendor_supply.csv        → Vendor delay + fulfilment risk
13. aop_plan.csv             → Full forecast deviation tracking
```

---

## 7. Key Calculations

### 7.1 DOI (Days of Inventory) — Backward Looking
```
DOI = (Store SOH + GIT Units) ÷ Avg Daily Sales (rolling 4-week actual)

Where:
  Store SOH  = store_inventory.inventory_qty
  GIT Units  = goods_in_transit.units_in_transit (transit_status = 'In Transit')
  Avg Daily  = sales_daily.net_sales_qty rolling 4-week avg ÷ 28
```

### 7.2 Days of Cover — Forward Looking
```
Days of Cover = (Store SOH + GIT Units) ÷ Forecasted Daily Sales Rate

Where:
  Forecasted Daily = aop_plan.planned_units ÷ days in period
                     OR rolling forecast if available

Alert: Days of Cover < days_of_cover_min → replenishment urgent
Alert: Days of Cover > days_of_cover_max → overstock, halt SO dispatch
```

### 7.3 MBQ Compliance
```
MBQ Breach = store_inventory.inventory_qty < article_mbq_by_store.mbq
             (or article_master.mbq if no store-specific rule exists)

Priority:
  P1 if SOH = 0 (stockout)
  P1 if SOH < 50% of MBQ
  P2 if SOH < MBQ but > 50% MBQ
  P3 if Days of Cover < days_of_cover_min but SOH ≥ MBQ
```

### 7.4 Pending SO
```
Pending SO = sales_orders WHERE so_status = 'Open'
             AND pick_deadline < TODAY
             AND units_picked = 0

Alert: Store X — Article Y — Z units on Pending SO — replenishment at risk
       Cross-check: if Store SOH also < MBQ → escalate to P1
```

---

## 8. Accepted File Formats (Current DSR|KRAI Support)

| Format | Supported | Notes |
|---|---|---|
| CSV | Yes | Preferred — auto-detected headers |
| Excel (.xlsx) | Yes | Multi-sheet support |
| JSON | Yes | Flat structure |
| Direct DB / API | Planned (Phase 3) | WMS / ERP / TMS connectors |

---

## 9. Data Quality Requirements

| Rule | Requirement |
|---|---|
| `store_id` | Consistent format across all files |
| `sku_id` | Lowest granularity — must match across Sales, Inventory, PO, SO, GIT |
| `article_id` | Must be consistent across all transactional files |
| `warehouse_id` | Must appear in Store Master (warehouse_id field) |
| `date` | ISO format: YYYY-MM-DD |
| `selling_floor_sqft` | Selling area only — not total GLA |
| `net_sales_qty` | Net of returns — gross qty minus returns qty |
| `gross_sales_amount` | Must be pre-discount, pre-promo gross value |
| `discount_amount` | Must not exceed gross_sales_amount |
| `promo_amount` | Must not exceed gross_sales_amount − discount_amount |
| `net_sales_amount` | Must equal gross_sales_amount − discount_amount − promo_amount |
| `discount_pct` + `promo_pct` | Combined must not exceed 100% |
| `inventory_qty` | End-of-day snapshot, not intra-day |
| `inventory_amount` | At cost price — not retail value |
| `gr_qty` | QC-accepted units only — not gross received (`gr_qty_gross`) |
| `gr_amount` | Accepted qty × cost price — not invoice amount |
| `so_status` | Must use defined values: Open / Pending / Picked / Dispatched / Received / Cancelled |
| `transit_status` | Must use: In Transit / Delivered / Delayed |
| `units_in_transit` | Only confirmed dispatched — not reserved |
| `mbq` | Must be > 0 for all active Articles — null MBQ disables replenishment trigger |
| `days_of_cover_min` | Must be set — drives replenishment urgency classification |
| `days_of_cover_max` | Must be set — drives overstock detection |
| `zone/region/state/city` | Must follow Store Master hierarchy — no ad hoc values |
| `division/section/department` | Must follow Article Master hierarchy — no ad hoc names |
| Nulls | `net_sales_qty`, `gross_sales_amount`, `net_sales_amount`, `inventory_qty`, `inventory_amount`, `gr_qty`, `gr_amount`, `units_in_transit`, `mbq` must not be null |
| Zero vs Null | `discount_pct`, `promo_pct` should be 0.0 when no discount/promo applied — not null |

---

## 10. Integration Approach (When Ready — Phase 3)

### Integration Plan
1. **File Drop** — Watched folder, auto-ingests on new file arrival
2. **Scheduled Pull** — Cron job pulls from shared drive / SFTP / WMS export
3. **Direct API** — WMS / ERP / TMS connector (REST or ODBC)
4. **Multi-source Merge** — Auto-join on common keys

### Join Keys
```
Sales           ←→ Store Inventory     : store_id + sku_id + date
Sales           ←→ Store Master        : store_id  [→ zone/region/state/city/location]
Sales           ←→ Article Master      : sku_id    [→ division/section/dept/article/option]
Sales           ←→ GIT                 : store_id + sku_id
Sales           ←→ Plan/AOP            : store_id + department_id + week/month
Store Inventory ←→ Article MBQ by Store: store_id + article_id
Store Inventory ←→ Article Master      : sku_id → article_id → division/section/dept
GR (WH)         ←→ Purchase Order      : po_number + sku_id
GR (WH)         ←→ Vendor              : vendor_id + po_number
SO              ←→ Store Inventory     : store_id + sku_id
SO              ←→ Warehouse Stock     : warehouse_id + sku_id
SO              ←→ Article MBQ by Store: store_id + article_id  [MBQ breach trigger]
GIT             ←→ SO                  : so_number
GIT             ←→ Store Master        : store_id + warehouse_id
Buying Plan     ←→ PO                  : article_id + sku_id + season
Article Master  ←→ Article MBQ by Store: article_id
Store Master    ←→ Article MBQ by Store: store_id
```

---

---

## 11. Pending — To Be Added Next Session

> These data entities are identified but not yet documented in full.
> **Complete these before integration planning begins.**

> **Note on Returns:**
> - **Customer Returns** — not required. System uses **Net Sales Amount & Qty** which already nets out returns.
> - **Vendor Returns** — not required. System uses **Actual Goods Receipt Amount & Qty** which already reflects only accepted stock.

| # | Entity | Priority | Why Needed |
|---|---|---|---|
| P1 | **Markdown / Price Event Data** | HIGH | Distinguishes organic Sell-Thru from markdown-driven Sell-Thru. SPSF drops on markdown. Without this: 95% Sell-Thru target has no quality signal. |
| P2 | **Trading / Season Calendar** | HIGH | Season-end date drives urgency of Days of Cover and Sell-Thru targets. Avg daily sales must exclude sale days and holidays. Without this: all time-based signals are miscalibrated. |
| P3 | **Open-to-Buy (OTB)** | MEDIUM | Financial governance on replenishment. OTB = budget available. Without this: RIECT can recommend SOs/POs that bust budget. |
| P4 | **Inter-Store Transfer** | MEDIUM | Stock moved Store→Store for zone/region balancing. Affects SOH at both stores. Not WH-origin GIT. |
| P5 | **Floor Space Plan / Range Plan** *(Article × Price Tier level)* | MEDIUM | Floor space planned at **Article level**, driven by **Price Tier (Eco / Regular / Popular / Premium)**. Higher MRP tier articles typically get more display space. Defines: which articles are ranged per store, planned display space per article (sq ft), display depth (units on display). Enables Article-level SPSF = Article Net Sales ÷ Article Floor Space. Display depth feeds MBQ calibration. Link: Article Master (`article_id` + `price_tier`) + Store Master (`store_id` + `tier`). |
| P6 | **Lost Sales / Stockout Log** | LOW | Stockout duration × avg sales rate = hidden revenue leakage signal |
| P7 | **Replenishment History** | LOW | SO fill rate tracking → rule engine tuning over time |

---

*Saved: 2026-02-20 | Updated: 2026-02-20 | DSR|KRAI → RIECT Data Integration Reference v1.7*
*Owner: Dinesh Srivastava*
*Related: docs/roadmap/RIECT-PLAN.md*
