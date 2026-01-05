PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;


-- A) DAILY SALES TREND (harian total)

DROP TABLE IF EXISTS elt_daily_sales_trend;

CREATE TABLE elt_daily_sales_trend AS
SELECT
  sale_date,
  COUNT(*) AS n_transactions,
  ROUND(SUM(gross_revenue), 2) AS daily_revenue,
  ROUND(AVG(trend_for_product), 2) AS avg_trend_for_product
FROM elt_fact_sales
GROUP BY sale_date
ORDER BY sale_date;


-- B) DAILY CATEGORY SALES TREND (harian per kategori)

DROP TABLE IF EXISTS elt_daily_category_sales_trend;

CREATE TABLE elt_daily_category_sales_trend AS
SELECT
  sale_date,
  product_category_mapped AS product_category,

  MIN(year)        AS year,
  MIN(month)       AS month,
  MIN(day_of_week) AS day_of_week,
  MIN(is_weekend)  AS is_weekend,

  COUNT(DISTINCT transaction_key) AS n_transactions,
  ROUND(SUM(transaction_qty), 2)  AS total_qty,
  ROUND(SUM(gross_revenue), 2)    AS daily_revenue,
  ROUND(AVG(gross_revenue), 2)    AS avg_revenue_per_tx,

  ROUND(AVG(trend_for_product), 2) AS avg_trend_for_product,
  ROUND(AVG(trend_avg), 2)         AS trend_avg_overall,
  ROUND(AVG(trend_max), 2)         AS trend_max_overall

FROM elt_fact_sales
GROUP BY sale_date, product_category_mapped
ORDER BY sale_date, product_category_mapped;

COMMIT;


