-- warehouse/analytical_queries.sql
PRAGMA foreign_keys = ON;

-- Q1 Total revenue
SELECT ROUND(SUM(gross_revenue), 2) AS total_revenue
FROM fact_sales;

-- Q2 Revenue by month
SELECT
  d.year,
  d.month,
  COUNT(f.transaction_key) AS n_transactions,
  ROUND(SUM(f.gross_revenue), 2) AS revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- Q3 Revenue by category
SELECT
  pc.product_category_mapped,
  COUNT(f.transaction_key) AS n_transactions,
  ROUND(SUM(f.gross_revenue), 2) AS revenue,
  ROUND(AVG(f.gross_revenue), 2) AS avg_revenue_per_tx
FROM fact_sales f
JOIN dim_product_category pc ON f.product_category_id = pc.product_category_id
GROUP BY pc.product_category_mapped
ORDER BY revenue DESC;

-- Q4 Avg revenue per unit by category
SELECT
  pc.product_category_mapped,
  ROUND(AVG(f.rev_per_unit), 3) AS avg_rev_per_unit
FROM fact_sales f
JOIN dim_product_category pc ON f.product_category_id = pc.product_category_id
GROUP BY pc.product_category_mapped
ORDER BY avg_rev_per_unit DESC;

-- Q5 Weekend vs weekday
SELECT
  d.is_weekend,
  COUNT(f.transaction_key) AS n_transactions,
  ROUND(SUM(f.gross_revenue), 2) AS revenue,
  ROUND(AVG(f.gross_revenue), 2) AS avg_revenue_per_tx
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.is_weekend
ORDER BY d.is_weekend;

-- Q6 Top 10 revenue days
SELECT
  d.sale_date,
  ROUND(SUM(f.gross_revenue), 2) AS revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.sale_date
ORDER BY revenue DESC
LIMIT 10;

-- Q7 Trend vs revenue daily (average trend_for_product)
SELECT
  d.sale_date,
  COUNT(f.transaction_key) AS n_transactions,
  ROUND(SUM(f.gross_revenue), 2) AS daily_revenue,
  ROUND(AVG(f.trend_for_product), 2) AS avg_trend_for_product
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.sale_date
ORDER BY d.sale_date;

-- Q8 Category trend avg vs revenue
SELECT
  pc.product_category_mapped,
  ROUND(AVG(f.trend_for_product), 2) AS avg_trend_for_product,
  ROUND(SUM(f.gross_revenue), 2) AS revenue
FROM fact_sales f
JOIN dim_product_category pc ON f.product_category_id = pc.product_category_id
GROUP BY pc.product_category_mapped
ORDER BY avg_trend_for_product DESC;

-- Q9 Join integrity check (FK completeness)
SELECT
  SUM(CASE WHEN d.date_key IS NULL THEN 1 ELSE 0 END) AS missing_date_fk,
  SUM(CASE WHEN pc.product_category_id IS NULL THEN 1 ELSE 0 END) AS missing_category_fk,
  SUM(CASE WHEN td.trend_id IS NULL THEN 1 ELSE 0 END) AS missing_trend_fk
FROM fact_sales f
LEFT JOIN dim_date d ON f.date_key = d.date_key
LEFT JOIN dim_product_category pc ON f.product_category_id = pc.product_category_id
LEFT JOIN dim_trend_daily td ON f.trend_id = td.trend_id;

-- Q10 Monthly category revenue share (raw revenue per category per month)
SELECT
  d.year,
  d.month,
  pc.product_category_mapped,
  ROUND(SUM(f.gross_revenue), 2) AS revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product_category pc ON f.product_category_id = pc.product_category_id
GROUP BY d.year, d.month, pc.product_category_mapped
ORDER BY d.year, d.month, revenue DESC;
