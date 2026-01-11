-- ============================================================
-- Exercise 4 — Customer Segmentation & Analytics (Custom Queries)
-- Schema: hm_dwh
--
-- This file contains 4 customer segmentation queries:
--  A) Revenue contribution by age range (with % share)
--  (B) Revenue contribution by Swedish region (with % share)
--  (C) Top product groups by age range (preference profiling)
--
-- All queries use schema-qualified names (hm_dwh.*) so they work
-- regardless of search_path settings in VS Code extensions.
-- ============================================================

-- ============================================================
-- (A) Revenue contribution by age range
-- Shows which age buckets generate the most sales and their % share.
-- ============================================================
WITH sales_by_age AS (
  SELECT
    c.age_range,
    SUM(f.price) AS revenue
  FROM hm_dwh.fact_sales f
  JOIN hm_dwh.dim_customer c ON c.customer_id = f.customer_id
  GROUP BY c.age_range
),
total AS (
  SELECT SUM(revenue) AS total_revenue
  FROM sales_by_age
)
SELECT
  s.age_range,
  s.revenue,
  ROUND(100.0 * s.revenue / t.total_revenue, 2) AS revenue_pct
FROM sales_by_age s
CROSS JOIN total t
ORDER BY s.revenue DESC;

-- ============================================================
-- (B) Revenue contribution by Swedish region
-- Which Swedish region contributes most (revenue + % share).
-- ============================================================
WITH sales_by_region AS (
  SELECT
    r.region_name,
    SUM(f.price) AS revenue
  FROM hm_dwh.fact_sales f
  JOIN hm_dwh.dim_customer c ON c.customer_id = f.customer_id
  JOIN hm_dwh.dim_region r ON r.region_id = c.region_id
  GROUP BY r.region_name
),
total AS (
  SELECT SUM(revenue) AS total_revenue
  FROM sales_by_region
)
SELECT
  s.region_name,
  s.revenue,
  ROUND(100.0 * s.revenue / t.total_revenue, 2) AS revenue_pct
FROM sales_by_region s
CROSS JOIN total t
ORDER BY s.revenue DESC;

-- ============================================================
-- (C) Customer preference segmentation: Top product groups by age range
-- Shows what each age group buys (top 5 product groups by revenue).
-- ============================================================
WITH sales_age_group AS (
  SELECT
    c.age_range,
    a.product_group_name,
    SUM(f.price) AS revenue
  FROM hm_dwh.fact_sales f
  JOIN hm_dwh.dim_customer c ON c.customer_id = f.customer_id
  JOIN hm_dwh.dim_article a ON a.article_id = f.article_id
  GROUP BY c.age_range, a.product_group_name
),
ranked AS (
  SELECT
    age_range,
    product_group_name,
    revenue,
    ROW_NUMBER() OVER (PARTITION BY age_range ORDER BY revenue DESC) AS rn
  FROM sales_age_group
)
SELECT
  age_range,
  product_group_name,
  revenue
FROM ranked
WHERE rn <= 5
ORDER BY age_range, revenue DESC;
