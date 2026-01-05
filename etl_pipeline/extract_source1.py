import os, json, time
from pathlib import Path

import numpy as np
import pandas as pd
import re

def extract_etl_source1() -> Path:
    url = "https://raw.githubusercontent.com/athabrani/Tugas-Besar-Big-Data-2025/main/raw/Coffee%20Shop%20Sales.csv"
    out_path = RAW_DIR / "source1_coffee_shop_sales_raw.csv"

    t0 = time.time()
    df = pd.read_csv(url)            # RAW
    df.to_csv(out_path, index=False) # RAW save
    exec_s = time.time() - t0

    append_jsonl(EXTRACT_LOG, {
        "stage": "extract",
        "source_name": "github_raw_csv_coffee_shop_sales",
        "output_file": str(out_path),
        "rows": int(df.shape[0]),
        "cols": int(df.shape[1]),
        "size_bytes": int(out_path.stat().st_size),
        "exec_seconds": round(exec_s, 4),
    })
    return out_path
