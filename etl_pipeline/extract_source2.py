import time, json
from pathlib import Path
import pandas as pd
from pytrends.request import TrendReq
from extract_source1 import append_jsonl

BASE_DIR = Path("/content/bigdata_final_project")
RAW_DIR = BASE_DIR / "raw"
LOG_DIR = BASE_DIR / "logs" / "etl"

EXTRACT_LOG = LOG_DIR / "extract_log.jsonl"

def derive_timeframe_from_sales(raw_sales_path: Path) -> str:
    tmp = pd.read_csv(raw_sales_path)
    tmp.columns = [c.strip().lower().replace(" ", "_") for c in tmp.columns]
    dt = pd.to_datetime(tmp["transaction_date"], errors="coerce")
    return f"{dt.min().date()} {dt.max().date()}"

def extract_etl_source2(raw_sales_path: Path) -> Path:
    keywords = ["coffee", "bakery", "tea", "chocolate"]
    geo = "US-NY"

    timeframe = derive_timeframe_from_sales(raw_sales_path)
    out_path = RAW_DIR / "source2_pytrends_interest_over_time_raw.csv"

    t0 = time.time()
    pytrends = TrendReq(hl="en-US", tz=420)
    pytrends.build_payload(keywords, timeframe=timeframe, geo=geo)
    iot = pytrends.interest_over_time()
    iot.to_csv(out_path, index=True)
    exec_s = time.time() - t0

    append_jsonl(EXTRACT_LOG, {
        "stage": "extract",
        "source_name": "google_trends_pytrends_interest_over_time",
        "params": {"keywords": keywords, "geo": geo, "timeframe": timeframe},
        "output_file": str(out_path),
        "rows": int(iot.shape[0]),
        "cols": int(iot.shape[1]),
        "size_bytes": int(out_path.stat().st_size),
        "exec_seconds": round(exec_s, 4),
    })
    return out_path
