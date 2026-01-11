# Exercise 4 — DWH Querying Customer Segmentation and Analytics

## Goal
Analyze customers based on their generated revenue. Define and execute at least **3** SQL queries that segment customers according to:
- customer characteristics (e.g., age range, region), and/or
- article preferences (e.g., most purchased product groups)

and show how each segment contributes to **aggregate sales**.

---

## SQL file (submitted)
The queries for this exercise are implemented in:

- `../sql/exercise4_customer_segm.sql`

---

## Queries included (overview)

### Query 1 — Revenue contribution by age range
**Purpose:** Determine which age groups contribute the most revenue and what percentage of total sales each group represents.

**Key outputs:**
- `age_range`
- `revenue`
- `revenue_pct`

---

### Query 2 — Revenue contribution by Swedish region
**Purpose:** Identify how total revenue is distributed across Swedish regions.

**Key outputs:**
- `region_name`
- `revenue`
- `revenue_pct`

---

### Query 3 — Customer preferences: top product groups by age range
**Purpose:** Show what each age group buys most (top 5 product groups by revenue per age range).

**Key outputs:**
- `age_range`
- `product_group_name`
- `revenue`

---

## How to run
1. Ensure Exercise 2 ETL has completed successfully and the tables contain data.
2. Execute the SQL file:
   - `../sql/exercise4_customer_segm.sql`

---

## Results (screenshots)

### Revenue contribution by age range
![A](exercise%204/A.png)

### Revenue contribution by region
![B](exercise%204/B.png)

### Top product groups by age range
![C](exercise%204/C.png)


---

## Notes / assumptions
- Revenue is defined as `SUM(fact_sales.price)` since each row represents one purchased item.
- Age ranges are derived during ETL and stored in `dim_customer.age_range`.
- Regions are derived from hex `postal_code` modulo 10 and stored via `dim_customer.region_id`.
