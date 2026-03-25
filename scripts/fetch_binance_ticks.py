import pandas as pd
import requests
import os
from datetime import datetime, timezone
import time

def fetch_binance_ticks(symbol, date_str):
    """
    Fetches historical trades from Binance API for a specific date.
    Note: For long history, Binance requires data download from their archive.
    This script tries to fetch recent ones via aggregator or archive links.
    """
    # Pattern: https://data.binance.vision/data/futures/um/daily/aggTrades/[SYMBOL]/[SYMBOL]-aggTrades-[DATE].zip
    url = f"https://data.binance.vision/data/futures/um/daily/aggTrades/{symbol}/{symbol}-aggTrades-{date_str}.zip"
    local_zip = f"data/{symbol}_binance_{date_str}.zip"
    local_csv = f"data/{symbol}_binance_{date_str}.csv"

    if os.path.exists(local_csv):
        print(f"Skipping {local_csv}, already exists.")
        return local_csv

    print(f"Downloading Binance {url}...")
    r = requests.get(url)
    if r.status_code == 200:
        os.makedirs("data", exist_ok=True)
        with open(local_zip, 'wb') as f:
            f.write(r.content)
        
        # Unzip
        import zipfile
        with zipfile.ZipFile(local_zip, 'r') as zip_ref:
            zip_ref.extractall("data")
            # Rename to standard
            extracted_name = zip_ref.namelist()[0]
            os.rename(f"data/{extracted_name}", local_csv)
            
        os.remove(local_zip)
        print(f"Successfully saved {local_csv}")
        return local_csv
    else:
        print(f"Failed to download from {url} (Status: {r.status_code})")
        return None

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="ONTUSDT")
    parser.add_argument("--date", default="2026-03-21")
    args = parser.parse_args()
    
    fetch_binance_ticks(args.symbol, args.date)
