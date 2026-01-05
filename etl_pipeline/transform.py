import os, json, time
from pathlib import Path

import numpy as np
import pandas as pd
import re

BASE_DIR = Path("/content/bigdata_final_project")
RAW_DIR = BASE_DIR / "raw"
LOG_DIR = BASE_DIR / "etl_pipeline" / "logs"
OUT_DIR = BASE_DIR / "etl_pipeline" / "output"

RAW_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

EXTRACT_LOG = LOG_DIR / "extract_log.jsonl"
TRANSFORM_LOG = LOG_DIR / "transform_log.jsonl"

def append_jsonl(path: Path, record: dict):
    record = dict(record)
    record["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
