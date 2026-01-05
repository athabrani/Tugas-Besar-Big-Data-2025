PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;


DROP TABLE IF EXISTS elt_dim_date;
DROP TABLE IF EXISTS elt_dim_product_category;
DROP TABLE IF EXISTS elt_dim_trend_daily;
DROP TABLE IF EXISTS elt_fact_sales;

DROP TABLE IF EXISTS elt_daily_sales_trend;
DROP TABLE IF EXISTS elt_daily_category_sales_trend;


DROP TABLE IF EXISTS raw_sales;
DROP TABLE IF EXISTS raw_trends;

COMMIT;
