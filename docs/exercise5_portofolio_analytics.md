# Exercise 5 — DWH Querying Article Portfolio Analytics

## Goal
Analyze article types and groups based on their generated revenue. Define and execute at least **3** SQL queries to analyze articles according to:
- their characteristics (product groups, types),
- their sales volume vs pricing behavior,
- their revenue contribution across regions.

---

## SQL file (submitted)
The queries for this exercise are implemented in:

- `../sql/exercise5_portfolio_analytics.sql`

> Note: The SQL uses schema-qualified names (`hm_dwh.*`) so it works without setting `search_path`.

---

## Queries included (overview)

### Query 1 — Top product groups by revenue + % share
**Purpose:** Identify the highest-revenue product groups and their percentage contribution to total revenue.

**Key outputs:**
- `product_group_name`
- `revenue`
- `revenue_pct`

---

### Query 2 — Underperformers: high units but low average price
**Purpose:** Find product groups that sell in large volumes but have relatively low average price, which can indicate a low-margin/high-volume portfolio area.

**Key outputs:**
- `product_group_name`
- `units_sold`
- `revenue`
- `avg_price`

---

### Query 3 — Regional portfolio differences: top product groups by region
**Purpose:** Compare which product groups drive revenue in each Swedish region (top 5 per region).

**Key outputs:**
- `region_name`
- `product_group_name`
- `revenue`

---

## How to run
1. Ensure Exercise 2 ETL has completed successfully and the tables contain data.
2. Execute the SQL file:
   - `../sql/exercise5_portfolio_analytics.sql`

---

## Results (screenshots)


### Top product groups by revenue + % share
![A](exercise%205/A.png)

### Underperformers: high units but low avg price
![B](exercise%205/B.png)

### Top product groups by region
![C](exercise%205/C.png)

---

## Notes / assumptions
- Revenue is defined as `SUM(fact_sales.price)` since each row represents one purchased item.
- The “underperformer” query uses a volume threshold (`HAVING COUNT(*) > 100000`).  
  This threshold is adjustable and was chosen to focus on genuinely high-volume product groups.
