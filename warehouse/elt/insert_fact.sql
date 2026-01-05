PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;

DELETE FROM fact_sales;

INSERT INTO fact_sales (
  transaction_key,
  date_key,
  product_category_id,
  trend_id,
  unit_price,
  transaction_qty,
  gross_revenue,
  rev_per_unit,
  trend_for_product
)
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
    LOWER(COALESCE(product_type,''))     AS product_type_raw,
    CAST(unit_price AS REAL)             AS unit_price,
    CAST(transaction_qty AS REAL)        AS transaction_qty
  FROM raw_sales
),
filtered AS (
  SELECT
    transaction_key,
    DATE(sale_date_str) AS sale_date,
    product_category_raw,
    product_type_raw,
    unit_price,
    transaction_qty
  FROM cleaned
  WHERE DATE(sale_date_str) IS NOT NULL
    AND product_category_raw NOT IN ('branded','flavours','flavors','housewares')
    AND product_type_raw NOT IN ('housewares','clothing')
),
mapped AS (
  SELECT
    transaction_key,
    sale_date,
    unit_price,
    transaction_qty,
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
),
final_mapped AS (
  SELECT *
  FROM mapped
  WHERE product_category_mapped IS NOT NULL
),
joined_dims AS (
  SELECT
    f.transaction_key,
    f.sale_date,
    CAST(STRFTIME('%Y%m%d', f.sale_date) AS INTEGER) AS date_key,
    pc.product_category_id,
    td.trend_id,

    f.unit_price,
    f.transaction_qty,
    (COALESCE(f.unit_price,0) * COALESCE(f.transaction_qty,0)) AS gross_revenue,

    CASE
      WHEN COALESCE(f.transaction_qty,0) != 0
      THEN (COALESCE(f.unit_price,0) * COALESCE(f.transaction_qty,0)) / f.transaction_qty
      ELSE 0
    END AS rev_per_unit,


    CASE f.product_category_mapped
      WHEN 'coffee' THEN COALESCE(td.coffee, 0)
      WHEN 'bakery' THEN COALESCE(td.bakery, 0)
      WHEN 'tea' THEN COALESCE(td.tea, 0)
      WHEN 'chocolate' THEN COALESCE(td.chocolate, 0)
      ELSE COALESCE(td.trend_avg, 0)
    END AS trend_for_product

  FROM final_mapped f
  JOIN dim_product_category pc
    ON pc.product_category_mapped = f.product_category_mapped
  LEFT JOIN dim_trend_daily td
    ON td.trend_date = f.sale_date
)
SELECT
  transaction_key,
  date_key,
  product_category_id,
  trend_id,
  unit_price,
  transaction_qty,
  gross_revenue,
  rev_per_unit,
  trend_for_product
FROM joined_dims
WHERE date_key IS NOT NULL AND date_key != 0;

COMMIT;
