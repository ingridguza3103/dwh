-- ============================================================
-- Exercise 5 — Article Portfolio Analytics (Custom Queries)
-- Schema: hm_dwh
--
-- This file contains 3 portfolio analytics queries:
--  (1) Top product groups by revenue + % share (portfolio leaders)
--  (9) Underperformers: product groups with high units but low avg price
-- (10) Regional portfolio differences: top product groups by region
--
-- All queries use schema-qualified names (hm_dwh.*).
-- ============================================================

-- ============================================================
-- (1) Top product groups by revenue + % share (portfolio leaders)
-- Shows the highest-revenue product groups and their contribution to total revenue.
-- ============================================================
WITH grp AS (
  SELECT
    a.product_group_name,
    SUM(f.price) AS revenue
  FROM hm_dwh.fact_sales f
  JOIN hm_dwh.dim_article a ON a.article_id = f.article_id
  GROUP BY a.product_group_name
),
tot AS (
  SELECT SUM(revenue) AS total_revenue
  FROM grp
)
SELECT
  g.product_group_name,
  g.revenue,
  ROUND(100.0 * g.revenue / (SELECT total_revenue FROM tot), 2) AS revenue_pct
FROM grp g
ORDER BY g.revenue DESC
LIMIT 20;

-- ============================================================
-- (9) Underperformers: product groups with high units but low avg price
-- Identifies product groups that sell in large volumes but have relatively low pricing.
-- The threshold (HAVING COUNT(*) > 100000) can be adjusted depending on desired strictness.
-- ============================================================
SELECT
  a.product_group_name,
  COUNT(*) AS units_sold,
  SUM(f.price) AS revenue,
  ROUND(AVG(f.price)::numeric, 2) AS avg_price
FROM hm_dwh.fact_sales f
JOIN hm_dwh.dim_article a ON a.article_id = f.article_id
GROUP BY a.product_group_name
HAVING COUNT(*) > 100000
ORDER BY avg_price ASC, revenue DESC;

-- ============================================================
-- (10) Regional portfolio differences: top product groups by region
-- Shows the top 5 product groups by revenue for each Swedish region.
-- Useful for identifying regional demand differences.
-- ============================================================
WITH rg AS (
  SELECT
    r.region_name,
    a.product_group_name,
    SUM(f.price) AS revenue
  FROM hm_dwh.fact_sales f
  JOIN hm_dwh.dim_customer c ON c.customer_id = f.customer_id
  JOIN hm_dwh.dim_region r ON r.region_id = c.region_id
  JOIN hm_dwh.dim_article a ON a.article_id = f.article_id
  GROUP BY r.region_name, a.product_group_name
),
ranked AS (
  SELECT
    region_name,
    product_group_name,
    revenue,
    ROW_NUMBER() OVER (PARTITION BY region_name ORDER BY revenue DESC) AS rn
  FROM rg
)
SELECT
  region_name,
  product_group_name,
  revenue
FROM ranked
WHERE rn <= 5
ORDER BY region_name, revenue DESC;
