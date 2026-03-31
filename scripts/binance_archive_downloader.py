import sys
import os
import requests
import zipfile
import io
import time
import pandas as pd
from datetime import datetime, timedelta

if sys.stdout.encoding.lower() != 'utf-8':
    try:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except:
        pass

def get_binance_vision_data(symbol, start_date_str, end_date_str, output_dir, market_type='um'):
    """
    Downloads free full historical data from Binance Public Archive.
    Target: Futures (um) or Spot Daily
    Types: Klines 1m and Trades
    """
    if market_type == 'spot':
        base_url = "https://data.binance.vision/data/spot/daily"
    else:
        base_url = "https://data.binance.vision/data/futures/um/daily"
        
    symbol_clean = symbol.replace("/", "").replace(":USDT", "").replace(":", "")
    if not symbol_clean.endswith("USDT"):
        symbol_clean += "USDT" # Ensures e.g. SOONUSDT
    print(f"\n--- [START] Starting Binance Archive Dump for {symbol_clean} ({market_type.upper()}) ---")
    
    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    save_dir = os.path.join(output_dir, symbol_clean, market_type)
    os.makedirs(save_dir, exist_ok=True)
    
    # 1. Download Klines (1m)
    print("\n[1/2] Fetching 1m Klines (OHLCV)...")
    klines_frames = []
    curr_dt = start_dt
    while curr_dt <= end_dt:
        date_str = curr_dt.strftime("%Y-%m-%d")
        url = f"{base_url}/klines/{symbol_clean}/1m/{symbol_clean}-1m-{date_str}.zip"
        
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                    for filename in z.namelist():
                        if filename.endswith(".csv"):
                            with z.open(filename) as f:
                                df = pd.read_csv(f, names=['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_vol', 'trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'])
                                klines_frames.append(df)
                print(f"  [OK] Fetched 1m klines for {date_str}")
            else:
                print(f"  [??] No klines for {date_str} (Status: {resp.status_code})")
        except Exception as e:
             print(f"  [Error] Error fetching {date_str}: {e}")
             
        curr_dt += timedelta(days=1)
        time.sleep(0.5)
        
    if klines_frames:
        merged_klines = pd.concat(klines_frames, ignore_index=True)
        merged_klines.sort_values('open_time', inplace=True)
        merged_klines.drop_duplicates(subset=['open_time'], inplace=True)
        out_csv = os.path.join(save_dir, "ohlcv_1m.csv")
        merged_klines.to_csv(out_csv, index=False)
        print(f"Merged total {len(merged_klines)} 1m candles to {out_csv}")
    
    # 2. Download Trades (Tick)
    print("\n[2/2] Fetching TICK Data (Trades)...")
    out_csv = os.path.join(save_dir, "trades_tick.csv")
    if os.path.exists(out_csv): os.remove(out_csv)
    
    curr_dt = start_dt - timedelta(days=1)
    header_written = False
    total_trades = 0
    
    while curr_dt <= end_dt:
        date_str = curr_dt.strftime("%Y-%m-%d")
        url = f"{base_url}/trades/{symbol_clean}/{symbol_clean}-trades-{date_str}.zip"
        
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                    for filename in z.namelist():
                        if filename.endswith(".csv"):
                            with z.open(filename) as f:
                                df = pd.read_csv(f, names=['id', 'price', 'qty', 'quote_qty', 'time', 'is_buyer_maker'])
                                df['time'] = pd.to_numeric(df['time'], errors='coerce')
                                df.to_csv(out_csv, mode='a', header=not header_written, index=False)
                                header_written = True
                                total_trades += len(df)
                print(f"  [OK] Fetched trades for {date_str} (Length: {len(df)})")
            else:
                print(f"  [??] No trades for {date_str} (Status: {resp.status_code})")
        except Exception as e:
             print(f"  [Error] Error fetching {date_str}: {e}")
             
        curr_dt += timedelta(days=1)
        time.sleep(0.5)
        
    print(f"\n[DONE] Saved total {total_trades} pure tick records to {out_csv}")

def main():
    # Test on Binance's massive pumper: SOON/USDT:USDT (pushed 49x over 44 days)
    symbol = "SOON"
    start_date = "2025-10-01"  
    end_date = "2025-11-15"
    
    out_dir = "data/binance_dna"
    get_binance_vision_data(symbol, start_date, end_date, out_dir)

if __name__ == "__main__":
    main()
