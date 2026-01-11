# Exercise 3 — OLAP Queries & Pivot (PostgreSQL)

## Goal
Write SQL queries on the DWH schema (`hm_dwh`) to produce:
- a pivot-style aggregation over Swedish region, time, and product type
- answers to queries (a)–(f) using the fact table and dimensions

## Files
- `sql/03_exercise3_queries.sql` — contains:
  - Pivot query (region x product type with time as season columns)
  - Queries (a)–(f) required by the assignment

## How to run
1. Connect to the PostgreSQL database where the ETL loaded the data.
2. Open `sql/03_exercise3_queries.sql` and run in SQL tool.
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

## Output
Running the queries returns:
- customer count with at least one purchase

![A](./exercise%203/A.png)

- total items sold in 2019

![B](./exercise%203/B.png)

- sales aggregates by graphical appearance

![C](./exercise%203/C.png)

- sales by region x product type x season

![D](./exercise%203/D.png)

- Stockholm sales by product group x month

![E](./exercise%203/E.png)

- top color group per season by total sales

![F](./exercise%203/F.png)
