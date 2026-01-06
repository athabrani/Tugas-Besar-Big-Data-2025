CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    sale_date TEXT,
    year INTEGER,
    month INTEGER,
    day_of_week INTEGER,
    is_weekend INTEGER
);

CREATE TABLE dim_product_category (
    product_category_id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_category_mapped TEXT UNIQUE
);

CREATE TABLE dim_trend (
    trend_id INTEGER PRIMARY KEY AUTOINCREMENT,
    coffee REAL,
    bakery REAL,
    tea REAL,
    chocolate REAL,
    trend_avg REAL,
    trend_max REAL,
    UNIQUE (coffee, bakery, tea, chocolate, trend_avg, trend_max)
);

CREATE TABLE fact_sales (
    transaction_key TEXT PRIMARY KEY,
    date_key INTEGER,
    product_category_id INTEGER,
    trend_id INTEGER,
    gross_revenue REAL,
    rev_per_unit REAL,
    trend_for_product REAL,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (product_category_id) REFERENCES dim_product_category(product_category_id),
    FOREIGN KEY (trend_id) REFERENCES dim_trend(trend_id)
);
