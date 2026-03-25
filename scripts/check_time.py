import pandas as pd
import json
from datetime import datetime
import os

def check():
    df = pd.read_csv('data/ml_dataset_v4.csv')
    max_ts_ms = df['timestamp'].max()
    min_ts_ms = df['timestamp'].min()
    
    print(f"Dataset Range (MS): {min_ts_ms} to {max_ts_ms}")
    print(f"Dataset Range (S): {min_ts_ms/1000} to {max_ts_ms/1000}")
    
    # User's Now (from metadata): 2026-03-23 10:04 JST
    # Assume 2026-03-23 10:04 JST is roughly 1774314240000 ms? (Guessing)
    
    now_jst = datetime(2026, 3, 23, 10, 4)
    # Approx UNIX for 3/23 10:04 JST (UTC+9)
    # 2026-03-23 01:04 UTC
    # 1742691840? Let's check.
    
    print(f"Max TS as JST: {datetime.fromtimestamp(max_ts_ms/1000) + pd.Timedelta(hours=9)}")
    print(f"Min TS as JST: {datetime.fromtimestamp(min_ts_ms/1000) + pd.Timedelta(hours=9)}")

if __name__ == '__main__':
    check()
