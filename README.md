# Retail SQL Pipeline — DuckDB + dbt

A zero-cost, local ELT that turns raw retail CSVs into clean, tested tables and business-ready marts:
- **Staging:** `stg_customers`, `stg_orders`, `stg_order_details`
- **Intermediate:** `int_order_revenue` (sums line items per order)
- **Marts:** `customer_360` (AOV, total revenue, repeat flag) and `daily_kpis` (orders/day, new vs repeat, revenue, AOV)

---

## What I built 

1. **Loaded raw CSVs** into a DuckDB warehouse.
2. **Standardized raw data** with dbt staging models (clean names, types, trims).
3. **Computed order revenue** from line items in an intermediate model.
4. **Published marts**:
   - `customer_360`: per-customer KPIs (order count, first/last order, **total_revenue**, **AOV**, **is_repeat_customer**).
   - `daily_kpis`: per-day KPIs (orders, unique customers, **new vs repeat**, **total_revenue**, **AOV**).
5. **Enforced data quality** with dbt tests (not_null, unique, relationships).
6. **Generated documentation** (dbt docs + lineage graph) for fast onboarding.

---

## Skills demonstrated

- **SQL**: joins, conditional aggregation, `date_trunc`, window functions (e.g., `row_number`), casting & type safety, revenue math.
- **Data Modeling (ELT)**: raw → **staging** → **intermediate** → **marts**; clear contracts and naming.
- **Data Quality**: dbt **schema tests** (not_null, unique, relationships) to protect KPIs.
- **Documentation & Lineage**: dbt **docs** and lineage graph to explain dependencies.
- **Performance Mindset**: pre-aggregating per-order revenue for faster daily rollups.
- **Version Control & Packaging**: Git/GitHub structure ready for CI (optional GitHub Actions).

---


Raw CSVs (customers, orders, products)
         │
         ▼
DuckDB warehouse (warehouse.duckdb)
         │
         ▼
Staging (stg_*) → clean names/types, trims, safe casts
         │
         ▼
Intermediate (int_order_revenue) → per-order revenue
         │
         ▼
Marts → customer_360 (AOV, total_revenue, repeat)
      → daily_kpis (orders/day, new vs repeat, revenue, AOV)
