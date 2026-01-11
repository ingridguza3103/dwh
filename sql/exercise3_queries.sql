-- ============================================================
-- Exercise 3 — Pivot + Queries (a-f)
-- Dataset: H&M 2019 DWH (Star Schema)
-- Schema: hm_dwh
-- ============================================================

SET search_path TO hm_dwh;

-- ------------------------------------------------------------
-- Pivot table requirement:
-- As per the exercise sheet "The data should now be shown as a pivot table with 
-- the dimensions of Swedish region, time and product type showing the aggregate sales transactions."
--
-- Pivot approach used here: conditional aggregation (PostgreSQL-friendly).
-- This pivot shows: Region x Product Type, with Seasons as columns.
-- (Time dimension represented by season; adjust to month/week if needed.)
-- ------------------------------------------------------------
SELECT
  r.region_name,
  a.product_type_name,
  SUM(CASE WHEN d.season = 'Winter' THEN f.price ELSE 0 END) AS winter_sales,
  SUM(CASE WHEN d.season = 'Spring' THEN f.price ELSE 0 END) AS spring_sales,
  SUM(CASE WHEN d.season = 'Summer' THEN f.price ELSE 0 END) AS summer_sales,
  SUM(CASE WHEN d.season = 'Autumn' THEN f.price ELSE 0 END) AS autumn_sales,
  SUM(f.price) AS total_sales
FROM fact_sales f
JOIN dim_customer c ON c.customer_id = f.customer_id
JOIN dim_region r ON r.region_id = c.region_id
JOIN dim_article a ON a.article_id = f.article_id
JOIN dim_date d ON d.date = f.date
GROUP BY r.region_name, a.product_type_name
ORDER BY r.region_name, a.product_type_name;

-- ============================================================
-- (a) How many customers did at least one purchase?
-- ============================================================
SELECT COUNT(DISTINCT customer_id) AS customers_with_purchase
FROM fact_sales;

-- ============================================================
-- (b) How many articles have been sold in 2019?
-- ============================================================
SELECT COUNT(*) AS articles_sold_units_2019
FROM fact_sales
WHERE date >= DATE '2019-01-01' AND date < DATE '2020-01-01';

-- ============================================================
-- (c) Aggregate sales by graphical appearance name.
-- ============================================================
SELECT
  a.graphical_appearance_name,
  SUM(f.price) AS total_sales
FROM fact_sales f
JOIN dim_article a ON a.article_id = f.article_id
GROUP BY a.graphical_appearance_name
ORDER BY total_sales DESC;

-- ============================================================
-- (d) Aggregate sales for each Swedish region by product type and season.
-- ============================================================
SELECT
  r.region_name,
  a.product_type_name,
  d.season,
  SUM(f.price) AS total_sales
FROM fact_sales f
JOIN dim_customer c ON c.customer_id = f.customer_id
JOIN dim_region r ON r.region_id = c.region_id
JOIN dim_article a ON a.article_id = f.article_id
JOIN dim_date d ON d.date = f.date
GROUP BY r.region_name, a.product_type_name, d.season
ORDER BY r.region_name, a.product_type_name, d.season;

-- ============================================================
-- (e) Aggregate sales for the region Stockholm by product category and month.
-- ============================================================
SELECT
  a.product_group_name AS product_category,
  d.month,
  SUM(f.price) AS total_sales
FROM fact_sales f
JOIN dim_customer c ON c.customer_id = f.customer_id
JOIN dim_region r ON r.region_id = c.region_id
JOIN dim_article a ON a.article_id = f.article_id
JOIN dim_date d ON d.date = f.date
WHERE r.region_name = 'Stockholm'
GROUP BY a.product_group_name, d.month
ORDER BY d.month, total_sales DESC;

-- ============================================================
-- (f) Color groups that led to highest aggregate sales per season.
-- Uses ROW_NUMBER() to pick the top color group per season.
-- ============================================================
WITH sales_by_season_color AS (
  SELECT
    d.season,
    a.colour_group_name,
    SUM(f.price) AS total_sales
  FROM fact_sales f
  JOIN dim_article a ON a.article_id = f.article_id
  JOIN dim_date d ON d.date = f.date
  GROUP BY d.season, a.colour_group_name
),
ranked AS (
  SELECT
    season,
    colour_group_name,
    total_sales,
    ROW_NUMBER() OVER (PARTITION BY season ORDER BY total_sales DESC) AS rn
  FROM sales_by_season_color
)
SELECT
  season,
  colour_group_name,
  total_sales
FROM ranked
WHERE rn = 1
ORDER BY season;
