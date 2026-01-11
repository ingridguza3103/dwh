-- DWH Star Schema for H&M 2019 dataset
-- Database: dwh
-- Schema: hm_dwh
-- Exercise 1 deliverable (schema only)

CREATE SCHEMA IF NOT EXISTS hm_dwh;
SET search_path TO hm_dwh;

-- DIM REGION
CREATE TABLE IF NOT EXISTS dim_region (
  region_id SMALLINT PRIMARY KEY CHECK (region_id BETWEEN 0 AND 9),
  region_name TEXT NOT NULL
);

INSERT INTO dim_region(region_id, region_name) VALUES
 (1, 'Stockholm'),
 (2, 'Södermanland / Östergötland'),
 (3, 'Jönköping'),
 (4, 'Skåne'),
 (5, 'Kronoberg / Kalmar'),
 (6, 'Värmland / Dalarna'),
 (7, 'Gävleborg / Västernorrland'),
 (8, 'Västerbotten / Norrbotten'),
 (9, 'Blekinge'),
 (0, 'Gotland')
ON CONFLICT (region_id) DO NOTHING;

-- DIM DATE
CREATE TABLE IF NOT EXISTS dim_date (
  date DATE PRIMARY KEY,
  weekday SMALLINT NOT NULL CHECK (weekday BETWEEN 1 AND 7),
  week_of_year SMALLINT NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
  month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
  season TEXT NOT NULL CHECK (season IN ('Winter','Spring','Summer','Autumn')),
  weather_code INTEGER NULL
);

-- DIM CUSTOMER
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_id TEXT PRIMARY KEY,
  fn SMALLINT NULL,
  active SMALLINT NULL,
  club_member_status TEXT NULL,
  fashion_news_frequency TEXT NULL,
  age SMALLINT NULL CHECK (age IS NULL OR age BETWEEN 0 AND 120),
  age_range TEXT NOT NULL,
  postal_code TEXT NULL,
  region_id SMALLINT NULL REFERENCES dim_region(region_id)
);

-- DIM ARTICLE
CREATE TABLE IF NOT EXISTS dim_article (
  article_id BIGINT PRIMARY KEY,
  product_code INTEGER NULL,
  prod_name TEXT NULL,

  product_type_no INTEGER NULL,
  product_type_name TEXT NULL,

  product_group_name TEXT NULL,

  graphical_appearance_no INTEGER NULL,
  graphical_appearance_name TEXT NULL,

  colour_group_code INTEGER NULL,
  colour_group_name TEXT NULL,

  detail_desc TEXT NULL
);

-- FACT SALES
CREATE TABLE IF NOT EXISTS fact_sales (
  fact_id BIGSERIAL PRIMARY KEY,

  date DATE NOT NULL REFERENCES dim_date(date),
  customer_id TEXT NOT NULL REFERENCES dim_customer(customer_id),
  article_id BIGINT NOT NULL REFERENCES dim_article(article_id),

  sales_channel_id SMALLINT NOT NULL,
  price NUMERIC(10,2) NOT NULL CHECK (price >= 0),
  quantity INTEGER NOT NULL DEFAULT 1 CHECK (quantity > 0)
);

-- INDEXES
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales(date);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_article ON fact_sales(article_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_channel ON fact_sales(sales_channel_id);
