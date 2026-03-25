import pandas as pd
import requests
import os
import gzip
import shutil
from datetime import datetime
import time

def download_bybit_ticks(symbol, date_str):
    """
    Downloads historical trade data from Bybit Public Archive.
    symbol: e.g. 'SIRENUSDT'
    date_str: e.g. '2026-02-24'
    """
    url = f"https://public.bybit.com/trading/{symbol}/{symbol}{date_str}.csv.gz"
    local_gz = f"data/{symbol}_{date_str}.csv.gz"
    local_csv = f"data/{symbol}_{date_str}.csv"
    
    if os.path.exists(local_csv):
        print(f"Skipping {local_csv}, already exists.")
        return local_csv

    print(f"Downloading {url}...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    r = requests.get(url, headers=headers, stream=True)
    if r.status_code == 200:
        os.makedirs("data", exist_ok=True)
        with open(local_gz, 'wb') as f:
            f.write(r.content)
        
        # Unzip
        with gzip.open(local_gz, 'rb') as f_in:
            with open(local_csv, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        os.remove(local_gz)
        print(f"Successfully saved {local_csv}")
        return local_csv
    else:
        print(f"Failed to download {url} (Status: {r.status_code})")
        return None

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="SIRENUSDT")
    parser.add_argument("--date", default="2026-02-24")
    args = parser.parse_args()
    
    download_bybit_ticks(args.symbol, args.date)
