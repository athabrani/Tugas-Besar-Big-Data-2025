import numpy as np
import pandas as pd
from pathlib import Path
from data_validation import validate_and_fix_data_quality

def load_raw_sources(raw_sales_path: Path, raw_trends_path: Path):
    sales = pd.read_csv(raw_sales_path)
    trends = pd.read_csv(raw_trends_path)

    sales.columns = [to_snake_case(c) for c in sales.columns]
    trends.columns = [to_snake_case(c) for c in trends.columns]

    return sales, trends

def define_primary_key_and_dedup(sales: pd.DataFrame):
    pk = "transaction_id" if "transaction_id" in sales.columns else None

    if pk is None:
        candidate = [c for c in ["transaction_date", "transaction_time", "store_location", "product_id", "product_type", "product_category"] if c in sales.columns]
        if not candidate:
            candidate = sales.columns[:3].tolist()
        sales["transaction_sk"] = pd.util.hash_pandas_object(sales[candidate], index=False).astype("int64")
        pk = "transaction_sk"

    before = len(sales)
    sales = sales.drop_duplicates(subset=[pk], keep="first")
    dup_removed = before - len(sales)

    return sales, pk, dup_removed

def handle_missing_values(sales: pd.DataFrame):
    for c in sales.columns:
        if sales[c].dtype.kind in "biufc":
            if sales[c].isna().any():
                sales[c] = sales[c].fillna(sales[c].median())
        else:
            if sales[c].isna().any():
                sales[c] = sales[c].fillna("unknown")
    return sales

def standardize_datetime(sales: pd.DataFrame):
    if "transaction_date" in sales.columns:
        sales["transaction_date"] = pd.to_datetime(sales["transaction_date"], errors="coerce")
    if "transaction_time" in sales.columns:
        sales["transaction_time"] = sales["transaction_time"].astype(str)
    return sales

def drop_unwanted_categories(sales: pd.DataFrame):
    # dataset biasanya punya kolom product_category dan/atau product_type
    if "product_category" in sales.columns:
        drop_vals = {"branded", "flavours", "flavors"}  # variasi ejaan
        mask = sales["product_category"].astype(str).str.lower().isin(drop_vals)
        sales = sales.loc[~mask].copy()

    return sales

def drop_unmapped_products(df: pd.DataFrame):
    before = len(df)
    df = df[df["product_category_mapped"].notna()].copy()
    dropped = before - len(df)
    return df, dropped


def outlier_and_normalize(sales: pd.DataFrame):
    numeric_cols = [c for c in sales.columns if sales[c].dtype.kind in "biufc"]
    prefer = [c for c in ["unit_price", "transaction_qty", "total_bill", "sales_amount", "total_amount"] if c in numeric_cols]
    target_outliers = (prefer + [c for c in numeric_cols if c not in prefer])[:2]

    outlier_cols_used = []
    for c in target_outliers:
        sales[c] = iqr_clip(sales[c])
        outlier_cols_used.append(c)

    norm_cols_used = []
    for c in target_outliers[:2]:
        sales[f"{c}_minmax"] = normalize_minmax(sales[c])
        norm_cols_used.append(c)

    return sales, outlier_cols_used, norm_cols_used


def encode_one_categorical(sales: pd.DataFrame):
    cat_candidates = [c for c in ["product_category", "product_type", "store_location"] if c in sales.columns]
    encoded_col = None

    if cat_candidates:
        encoded_col = cat_candidates[0]
        sales[encoded_col] = sales[encoded_col].astype(str)
        ohe = pd.get_dummies(sales[encoded_col], prefix=encoded_col)
        sales = pd.concat([sales.drop(columns=[encoded_col]), ohe], axis=1)

    return sales, encoded_col


def prepare_trends_date(trends: pd.DataFrame):
    if "date" in trends.columns:
        trend_date_col = "date"
    elif "unnamed:_0" in trends.columns:
        trend_date_col = "unnamed:_0"
    elif "unnamed:0" in trends.columns:
        trend_date_col = "unnamed:0"
    else:
        trend_date_col = trends.columns[0]

    trends = trends.rename(columns={trend_date_col: "trend_date"})
    trends["trend_date"] = pd.to_datetime(trends["trend_date"], errors="coerce")
    return trends

def join_sales_trends_by_date(sales: pd.DataFrame, trends: pd.DataFrame):
    if "transaction_date" not in sales.columns:
        return sales.copy()

    sales["sale_date"] = sales["transaction_date"].dt.date
    trends["trend_date_only"] = trends["trend_date"].dt.date

    trend_value_cols = [c for c in trends.columns if c not in ("trend_date", "trend_date_only") and c != "ispartial"]
    trends_daily = trends.groupby("trend_date_only")[trend_value_cols].mean().reset_index()

    merged = sales.merge(trends_daily, how="left", left_on="sale_date", right_on="trend_date_only").drop(columns=["trend_date_only"])
    return merged


def add_features(merged: pd.DataFrame):
    # time features
    if "transaction_date" in merged.columns and pd.api.types.is_datetime64_any_dtype(merged["transaction_date"]):
        merged["year"] = merged["transaction_date"].dt.year
        merged["month"] = merged["transaction_date"].dt.month
        merged["day_of_week"] = merged["transaction_date"].dt.dayofweek
        merged["is_weekend"] = merged["day_of_week"].isin([5, 6]).astype(int)
        merged["date_key"] = pd.to_numeric(merged["transaction_date"].dt.strftime("%Y%m%d"), errors="coerce").fillna(0).astype("int64")
    else:
        merged["year"] = 0
        merged["month"] = 0
        merged["day_of_week"] = 0
        merged["is_weekend"] = 0
        merged["date_key"] = 0

    # revenue
    if "unit_price" in merged.columns and "transaction_qty" in merged.columns:
        merged["gross_revenue"] = pd.to_numeric(merged["unit_price"], errors="coerce") * pd.to_numeric(merged["transaction_qty"], errors="coerce")
    elif "total_bill" in merged.columns:
        merged["gross_revenue"] = pd.to_numeric(merged["total_bill"], errors="coerce")
    else:
        merged["gross_revenue"] = 0.0

    merged["rev_per_unit"] = np.where(
        pd.to_numeric(merged.get("transaction_qty", 0), errors="coerce").fillna(0) > 0,
        merged["gross_revenue"].fillna(0) / pd.to_numeric(merged.get("transaction_qty", 1), errors="coerce").replace(0, np.nan),
        0.0
    )
    merged["rev_per_unit"] = merged["rev_per_unit"].fillna(0.0)

    # trend summary
    trend_cols = [c for c in ["coffee", "bakery", "tea", "chocolate"] if c in merged.columns]
    if trend_cols:
        merged["trend_avg"] = merged[trend_cols].mean(axis=1)
        merged["trend_max"] = merged[trend_cols].max(axis=1)
    else:
        merged["trend_avg"] = np.nan
        merged["trend_max"] = np.nan

    return merged


def add_product_category_mapped(merged: pd.DataFrame, raw_sales_path: Path, pk: str):
    raw_sales_for_map = pd.read_csv(raw_sales_path)
    raw_sales_for_map.columns = [to_snake_case(c) for c in raw_sales_for_map.columns]

    map_col = "product_type" if "product_type" in raw_sales_for_map.columns else None
    if map_col is None:
        map_col = "product_detail" if "product_detail" in raw_sales_for_map.columns else None

    if map_col:
        mapper = raw_sales_for_map[[pk, map_col]].drop_duplicates(subset=[pk])
        mapper["product_category_mapped"] = mapper[map_col].apply(map_product_to_category)
        merged = merged.merge(mapper[[pk, "product_category_mapped"]], how="left", on=pk)
    else:
        merged["product_category_mapped"] = "other"

    return merged

def add_trend_for_product(merged: pd.DataFrame):
    for c in ["coffee", "bakery", "tea", "chocolate"]:
        if c not in merged.columns:
            merged[c] = np.nan

    def pick_trend(row):
        cat = row.get("product_category_mapped", "other")
        if cat in ["coffee", "bakery", "tea", "chocolate"]:
            return row.get(cat, np.nan)
        return np.nan

    merged["trend_for_product"] = merged.apply(pick_trend, axis=1)

    if "trend_avg" in merged.columns:
        merged["trend_for_product"] = merged["trend_for_product"].fillna(merged["trend_avg"])
    merged["trend_for_product"] = merged["trend_for_product"].fillna(0)

    return merged


def validate_and_fix_data_quality(merged: pd.DataFrame, pk: str):
    dq_results = []

    # 1 uniqueness
    unique_ok = merged[pk].is_unique
    dq_results.append({"rule": "uniqueness_check", "target": pk, "ok": bool(unique_ok)})
    if not unique_ok:
        merged = merged.drop_duplicates(subset=[pk], keep="first")

    # 2 null check
    critical_cols = [c for c in [pk, "date_key", "gross_revenue"] if c in merged.columns]
    null_ok = not merged[critical_cols].isnull().any().any()
    dq_results.append({"rule": "null_check", "target": critical_cols, "ok": bool(null_ok)})
    if not null_ok:
        merged = merged[merged[pk].notna()]
        merged["gross_revenue"] = merged["gross_revenue"].fillna(0.0)
        merged["date_key"] = merged["date_key"].fillna(0).astype("int64")

    # 3 range check
    range_ok = (merged["gross_revenue"].fillna(0) >= 0).all()
    dq_results.append({"rule": "range_check", "target": "gross_revenue>=0", "ok": bool(range_ok)})
    if not range_ok:
        merged.loc[merged["gross_revenue"] < 0, "gross_revenue"] = 0.0

    # 4 datatype consistency
    try:
        merged["date_key"] = merged["date_key"].astype("int64")
        dtype_ok_date_key = True
    except:
        dtype_ok_date_key = False
        merged["date_key"] = pd.to_numeric(merged["date_key"], errors="coerce").fillna(0).astype("int64")
    dq_results.append({"rule": "datatype_consistency", "target": "date_key:int64", "ok": bool(dtype_ok_date_key)})

    # 5 referential integrity
    if "transaction_date" in merged.columns:
        ref_ok = ((merged["date_key"] != 0) == merged["transaction_date"].notna()).all()
    else:
        ref_ok = True
    dq_results.append({"rule": "referential_integrity", "target": "date_key <-> transaction_date", "ok": bool(ref_ok)})
    if not ref_ok and "transaction_date" in merged.columns:
        merged.loc[merged["transaction_date"].isna(), "date_key"] = 0

    # 6 distribution check
    s = merged["gross_revenue"].dropna()
    if len(s) > 0:
        p99 = s.quantile(0.99)
        med = s.median()
        dist_ok = True if med == 0 else (p99 / med <= 50)
    else:
        dist_ok = False
        p99 = np.nan
    dq_results.append({"rule": "distribution_check", "target": "gross_revenue p99/median<=50", "ok": bool(dist_ok)})
    if not dist_ok and len(s) > 0:
        merged["gross_revenue"] = merged["gross_revenue"].clip(upper=p99)

    return merged, dq_results


def final_nan_cleanup(df: pd.DataFrame):
    before = len(df)

    # 1. Wajib punya kategori
    df = df[df["product_category_mapped"].notna()].copy()

    # 2. Pastikan numeric penting tidak NaN
    df["gross_revenue"] = df["gross_revenue"].fillna(0)
    df["rev_per_unit"] = df["rev_per_unit"].fillna(0)
    df["trend_for_product"] = df["trend_for_product"].fillna(0)

    # 3. Date key wajib valid
    df = df[df["date_key"] != 0]

    dropped = before - len(df)
    return df, dropped


def transform_etl(raw_sales_path: Path, raw_trends_path: Path) -> Path:
    t0 = time.time()

    # 1 load + snake_case
    sales, trends = load_raw_sources(raw_sales_path, raw_trends_path)

    # 2 PK + dedup
    sales, pk, dup_removed = define_primary_key_and_dedup(sales)

    # 3 missing values
    sales = handle_missing_values(sales)

    # 4 datetime standardization
    sales = standardize_datetime(sales)

    sales = drop_unwanted_categories(sales)

    # 5 outlier + normalization
    sales, outlier_cols_used, norm_cols_used = outlier_and_normalize(sales)

    # 6 encoding
    sales, encoded_col = encode_one_categorical(sales)

    # 7 prepare trends + join
    trends = prepare_trends_date(trends)
    merged = join_sales_trends_by_date(sales, trends)

    # 8 features
    merged = add_features(merged)

    # 9 mapping kategori + trend_for_product
    merged = add_product_category_mapped(merged, raw_sales_path, pk)
    merged, dropped_unmapped = drop_unmapped_products(merged)
    merged = add_trend_for_product(merged)

    # 10 data quality validation + fixes
    merged, dq_results = validate_and_fix_data_quality(merged, pk)

    merged, dq_results = validate_and_fix_data_quality(merged, pk)

    # 11 FINAL NaN CLEANUP
    merged, nan_dropped_rows = final_nan_cleanup(merged)

    # trend_nan_rate
    trend_nan_rate = {c: float(merged[c].isna().mean()) for c in ["coffee","bakery","tea","chocolate"] if c in merged.columns}

    # save output
    out_path = OUT_DIR / "etl_transformed_sales_enriched.csv"
    merged.to_csv(out_path, index=False)

    append_jsonl(TRANSFORM_LOG, {
        "stage": "transform",
        "input_files": [str(raw_sales_path), str(raw_trends_path)],
        "output_file": str(out_path),
        "rows": int(merged.shape[0]),
        "cols": int(merged.shape[1]),
        "pk_used": pk,
        "dup_removed": int(dup_removed),
        "outlier_method": "IQR clipping (1.5*IQR)",
        "outlier_cols_used": outlier_cols_used,
        "normalized_cols": norm_cols_used,
        "encoded_col_original": encoded_col,
        "trend_nan_rate": trend_nan_rate,
        "dq_rules": dq_results,
        "exec_seconds": round(time.time() - t0, 4),
        "dropped_unmapped_products": int(dropped_unmapped),
    })

    before_drop = len(sales)
    sales = drop_unwanted_categories(sales)
    dropped_rows = before_drop - len(sales)

    return out_path


def build_daily_category_aggregate(df: pd.DataFrame):
    df = df.copy()

    # pastikan sale_date ada
    if "transaction_date" in df.columns:
        df["sale_date"] = pd.to_datetime(df["transaction_date"]).dt.date
    else:
        df["sale_date"] = pd.to_datetime(df["date_key"].astype(str), format="%Y%m%d").dt.date

    agg = (
        df
        .groupby(["sale_date", "product_category_mapped"])
        .agg(
            year=("year", "first"),
            month=("month", "first"),
            day_of_week=("day_of_week", "first"),
            is_weekend=("is_weekend", "first"),

            n_transactions=("transaction_key", "count"),
            total_qty=("transaction_qty", "sum"),
            daily_revenue=("gross_revenue", "sum"),

            avg_revenue_per_tx=("gross_revenue", "mean"),
            avg_trend_for_product=("trend_for_product", "mean"),

            trend_avg_overall=("trend_avg", "mean"),
            trend_max_overall=("trend_max", "max"),
        )
        .reset_index()
    )

    # rounding biar rapi
    for c in [
        "daily_revenue",
        "avg_revenue_per_tx",
        "avg_trend_for_product",
        "trend_avg_overall",
        "trend_max_overall",
    ]:
        if c in agg.columns:
            agg[c] = agg[c].round(2)

    return agg

transformed_path = transform_etl(raw_sales_path, raw_trends_path)
df_transformed = pd.read_csv(transformed_path)

transformed_path, df_transformed.shape

trend_cols = [c for c in ["coffee", "bakery", "tea", "chocolate"] if c in df_transformed.columns]
df_transformed[trend_cols].isna().mean().sort_values(ascending=False)


df_transformed[["product_category_mapped", "trend_for_product"]]

df_daily_category = build_daily_category_aggregate(df_transformed)

out_agg_path = OUT_DIR / "mart_daily_category_sales.csv"
df_daily_category.to_csv(out_agg_path, index=False)

df_daily_category.head(20)

print("=== EXTRACT LOG (first 2 lines) ===")
with open(EXTRACT_LOG, "r", encoding="utf-8") as f:
    for i in range(2):
        print(f.readline().strip())

print("\n=== TRANSFORM LOG (last line) ===")
!tail -n 1 /content/bigdata_final_project/etl_pipeline/logs/transform_log.jsonl

tmp = pd.read_csv(raw_sales_path)
tmp.columns = [to_snake_case(c) for c in tmp.columns]
dt = pd.to_datetime(tmp["transaction_date"], errors="coerce")
print("Parse success ratio:", dt.notna().mean())
print("Min/max:", dt.min(), dt.max())

