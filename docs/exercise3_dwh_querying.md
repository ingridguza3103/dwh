# Exercise 3 — OLAP Queries & Pivot (PostgreSQL)

## Goal
Write SQL queries on the DWH schema (`hm_dwh`) to produce:
- a pivot-style aggregation over Swedish region, time, and product type
- answers to queries (a)–(f) using the fact table and dimensions

## Files
- `sql/03_exercise3_queries.sql` — contains:
  - Pivot query (region × product type with time as season columns)
  - Queries (a)–(f) required by the assignment

## How to run
1. Connect to the PostgreSQL database where the ETL loaded the data.
2. Open `sql/03_exercise3_queries.sql` in VS Code (PostgreSQL extension).
3. Execute each query (highlight and run), or run the whole file.

## Pivot table implementation
The assignment requests a pivot table over:
- Swedish region
- time
- product type
showing aggregate sales.

In PostgreSQL, a pivot is implemented via **conditional aggregation**:
- rows: `region_name`, `product_type_name`
- columns: seasons (`Winter`, `Spring`, `Summer`, `Autumn`)
- measure: `SUM(price)` (total sales)

This produces a pivot-like result without requiring additional extensions.

## Query notes / assumptions
- **(b) “How many articles sold”** is interpreted as:
  - total units sold = `COUNT(*)` over fact rows in 2019  
  Additionally, an optional query for distinct SKUs (`COUNT(DISTINCT article_id)`) is included.
- **(e) “Product category”** is interpreted as `product_group_name` (retail-level category).  
  If required, it can be changed to `product_type_name`.

## Output
Running the queries returns:
- customer count with at least one purchase
- total items sold in 2019
- sales aggregates by graphical appearance
- sales by region × product type × season
- Stockholm sales by product group × month
- top color group per season by total sales
