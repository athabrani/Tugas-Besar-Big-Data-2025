PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;


DELETE FROM dim_trend_daily;
DELETE FROM dim_date;
DELETE FROM dim_product_category;

-- A) dim_trend_daily from raw_trends
INSERT INTO dim_trend_daily (trend_date, coffee, bakery, tea, chocolate, trend_avg, trend_max)
WITH base AS (
  SELECT
    DATE(trend_date) AS trend_date,
    AVG(coffee)    AS coffee,
    AVG(bakery)    AS bakery,
    AVG(tea)       AS tea,
    AVG(chocolate) AS chocolate
  FROM raw_trends
  WHERE DATE(trend_date) IS NOT NULL
  GROUP BY DATE(trend_date)
),
final AS (
  SELECT
    trend_date,
    coffee, bakery, tea, chocolate,
    (COALESCE(coffee,0)+COALESCE(bakery,0)+COALESCE(tea,0)+COALESCE(chocolate,0))/4.0 AS trend_avg,
    CASE
      WHEN COALESCE(coffee,0) >= COALESCE(bakery,0)
       AND COALESCE(coffee,0) >= COALESCE(tea,0)
       AND COALESCE(coffee,0) >= COALESCE(chocolate,0) THEN COALESCE(coffee,0)
      WHEN COALESCE(bakery,0) >= COALESCE(tea,0)
       AND COALESCE(bakery,0) >= COALESCE(chocolate,0) THEN COALESCE(bakery,0)
      WHEN COALESCE(tea,0) >= COALESCE(chocolate,0) THEN COALESCE(tea,0)
      ELSE COALESCE(chocolate,0)
    END AS trend_max
  FROM base
)
SELECT * FROM final;


-- B) dim_date from raw_sales
INSERT INTO dim_date (date_key, sale_date, year, month, day_of_week, is_weekend)
WITH cleaned AS (
  SELECT
    CAST(transaction_id AS TEXT) AS transaction_key,

    CASE
      WHEN instr(trim(transaction_date), '/') > 0 THEN
        printf(
          '%04d-%02d-%02d',
          CAST(
            substr(
              substr(trim(transaction_date), instr(trim(transaction_date), '/') + 1),
              instr(substr(trim(transaction_date), instr(trim(transaction_date), '/') + 1), '/') + 1
            ) AS INTEGER
          ),
          CAST(substr(trim(transaction_date), 1, instr(trim(transaction_date), '/') - 1) AS INTEGER),
          CAST(
            substr(
              substr(trim(transaction_date), instr(trim(transaction_date), '/') + 1),
              1,
              instr(substr(trim(transaction_date), instr(trim(transaction_date), '/') + 1), '/') - 1
            ) AS INTEGER
          )
        )
      ELSE
        substr(trim(transaction_date), 1, 10)
    END AS sale_date_str,

    LOWER(COALESCE(product_category,'')) AS product_category_raw,
    LOWER(COALESCE(product_type,''))     AS product_type_raw
  FROM raw_sales
),
filtered AS (
  SELECT
    DATE(sale_date_str) AS sale_date,
    product_category_raw,
    product_type_raw
  FROM cleaned
  WHERE DATE(sale_date_str) IS NOT NULL
    AND product_category_raw NOT IN ('branded','flavours','flavors','housewares')
    AND product_type_raw NOT IN ('housewares','clothing')
),
date_distinct AS (
  SELECT DISTINCT sale_date FROM filtered
)
SELECT
  CAST(STRFTIME('%Y%m%d', sale_date) AS INTEGER) AS date_key,
  sale_date,
  CAST(STRFTIME('%Y', sale_date) AS INTEGER) AS year,
  CAST(STRFTIME('%m', sale_date) AS INTEGER) AS month,
  CAST(STRFTIME('%w', sale_date) AS INTEGER) AS day_of_week,
  CASE WHEN CAST(STRFTIME('%w', sale_date) AS INTEGER) IN (0,6) THEN 1 ELSE 0 END AS is_weekend
FROM date_distinct
ORDER BY date_key;


-- C) dim_product_category from mapping product_type_raw -> product_category_mapped
INSERT INTO dim_product_category (product_category_mapped)
WITH cleaned AS (
  SELECT
    CASE
      WHEN instr(trim(transaction_date), '/') > 0 THEN
        printf(
          '%04d-%02d-%02d',
          CAST(
            substr(
              substr(trim(transaction_date), instr(trim(transaction_date), '/') + 1),
              instr(substr(trim(transaction_date), instr(trim(transaction_date), '/') + 1), '/') + 1
            ) AS INTEGER
          ),
          CAST(substr(trim(transaction_date), 1, instr(trim(transaction_date), '/') - 1) AS INTEGER),
          CAST(
            substr(
              substr(trim(transaction_date), instr(trim(transaction_date), '/') + 1),
              1,
              instr(substr(trim(transaction_date), instr(trim(transaction_date), '/') + 1), '/') - 1
            ) AS INTEGER
          )
        )
      ELSE
        substr(trim(transaction_date), 1, 10)
    END AS sale_date_str,

    LOWER(COALESCE(product_category,'')) AS product_category_raw,
    LOWER(COALESCE(product_type,''))     AS product_type_raw
  FROM raw_sales
),
filtered AS (
  SELECT
    DATE(sale_date_str) AS sale_date,
    product_category_raw,
    product_type_raw
  FROM cleaned
  WHERE DATE(sale_date_str) IS NOT NULL
    AND product_category_raw NOT IN ('branded','flavours','flavors','housewares')
    AND product_type_raw NOT IN ('housewares','clothing')
),
mapped AS (
  SELECT
    CASE
      WHEN product_type_raw LIKE '%coffee%'
        OR product_type_raw LIKE '%espresso%'
        OR product_type_raw LIKE '%latte%'
        OR product_type_raw LIKE '%cappuccino%'
        OR product_type_raw LIKE '%americano%'
        OR product_type_raw LIKE '%macchiato%'
        OR product_type_raw LIKE '%cold brew%'
        OR product_type_raw LIKE '%mocha%'
        OR product_type_raw LIKE '%drip%'
        OR product_type_raw LIKE '%beans%'
      THEN 'coffee'

      WHEN product_type_raw LIKE '%tea%'
        OR product_type_raw LIKE '%chai%'
        OR product_type_raw LIKE '%matcha%'
        OR product_type_raw LIKE '%earl%'
        OR product_type_raw LIKE '%herbal%'
      THEN 'tea'

      WHEN product_type_raw LIKE '%chocolate%'
        OR product_type_raw LIKE '%cocoa%'
      THEN 'chocolate'

      WHEN product_type_raw LIKE '%bakery%'
        OR product_type_raw LIKE '%croissant%'
        OR product_type_raw LIKE '%muffin%'
        OR product_type_raw LIKE '%cookie%'
        OR product_type_raw LIKE '%cake%'
        OR product_type_raw LIKE '%pastry%'
        OR product_type_raw LIKE '%bread%'
        OR product_type_raw LIKE '%donut%'
        OR product_type_raw LIKE '%brownie%'
        OR product_type_raw LIKE '%scone%'
        OR product_type_raw LIKE '%biscotti%'
      THEN 'bakery'

      ELSE NULL
    END AS product_category_mapped
  FROM filtered
)
SELECT DISTINCT product_category_mapped
FROM mapped
WHERE product_category_mapped IS NOT NULL
ORDER BY product_category_mapped;

COMMIT;
