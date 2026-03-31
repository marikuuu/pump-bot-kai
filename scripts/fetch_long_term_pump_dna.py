import ccxt
import pandas as pd
import os
import time
from datetime import datetime, timedelta

def find_bottom_and_peak(exchange, symbol, timeframe='1d', lookback_days=90):
    """Finds the peak and the preceding absolute bottom over the past N days."""
    since = int((datetime.now() - timedelta(days=lookback_days)).timestamp() * 1000)
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=1000)
    df = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
    
    if df.empty:
        return None, None
        
    # Find the maximum peak
    peak_idx = df['h'].idxmax()
    peak_ts = df.iloc[peak_idx]['ts']
    
    # Trace backward to find the absolute bottom that started the run
    # (look for the minimum low *before* the peak)
    pre_peak_df = df.iloc[:peak_idx+1]
    if pre_peak_df.empty:
        bottom_ts = df.iloc[0]['ts']
    else:
        bottom_idx = pre_peak_df['l'].idxmin()
        bottom_ts = pre_peak_df.iloc[bottom_idx]['ts']
        
    return int(bottom_ts), int(peak_ts)

def fetch_long_term_data():
    binance = ccxt.binance({'options': {'defaultType': 'future'}})
    mexc = ccxt.mexc({'options': {'defaultType': 'swap'}})
    
    # Focus on the user's highest priority coins and a few others
    targets = ["POWER", "SIREN", "AIN"]
    
    base_dir = "data/naked_dna_long"
    os.makedirs(base_dir, exist_ok=True)
    
    for base_sym in targets:
        symbol = f"{base_sym}/USDT"
        ex = ccxt.mexc({'options': {'defaultType': 'spot'}})
        try:
            ex.load_markets()
            if symbol not in ex.symbols:
                print(f"Skipping {symbol}, not found on MEXC Spot.")
                continue
        except Exception as e:
            print(f"Error loading markets: {e}")
            continue
            
        print(f"\n🚀 Processing {symbol} on {ex.id}...")
        
        # 1. Discover peak and bottom
        bottom_ts, peak_ts = find_bottom_and_peak(ex, symbol, timeframe='1d', lookback_days=120)
        
        if bottom_ts is None or peak_ts is None:
            print(f"  ❌ Could not find reliable historical points for {symbol}")
            continue
            
        # Add 3 days buffer before bottom and 1 day after peak
        start_ts = bottom_ts - (3 * 24 * 60 * 60 * 1000)
        end_ts = peak_ts + (1 * 24 * 60 * 60 * 1000)
        
        start_dt = datetime.fromtimestamp(start_ts / 1000)
        end_dt = datetime.fromtimestamp(end_ts / 1000)
        print(f"  📍 Analysis Window: {start_dt} to {end_dt} (Duration: {(end_ts - start_ts)/(1000*60*60*24):.1f} days)")
        
        out_path = os.path.join(base_dir, f"{symbol.replace('/', '_').replace(':', '_')}")
        os.makedirs(out_path, exist_ok=True)
        
        # 2. Fetch 1m OHLCV
        print(f"  Fetching 1m OHLCV...")
        all_ohlcv = []
        cur_ts = start_ts
        while cur_ts < end_ts:
            try:
                ohlcv = ex.fetch_ohlcv(symbol, timeframe='1m', since=cur_ts, limit=1000)
                if not ohlcv: break
                all_ohlcv.extend(ohlcv)
                cur_ts = ohlcv[-1][0] + 60000
                time.sleep(0.1) # Soft rate limit
            except Exception as e:
                print(f"  OHLCV Error: {e}")
                time.sleep(1)
                
        if all_ohlcv:
            df_k = pd.DataFrame(all_ohlcv, columns=['ts','o','h','l','c','v'])
            df_k.drop_duplicates(subset=['ts'], inplace=True)
            df_k.to_csv(f"{out_path}/ohlcv_1m.csv", index=False)
            print(f"  ✅ Saved {len(df_k)} 1m candles.")
        
        # 3. Fetch OI History (if available)
        print(f"  Fetching OI History...")
        oi_history = []
        cur_ts = start_ts
        while cur_ts < end_ts:
            try:
                if 'fetch_open_interest_history' in ex.has and ex.has['fetch_open_interest_history']:
                    oi = ex.fetch_open_interest_history(symbol, timeframe='5m', since=cur_ts, limit=500)
                    if not oi: break
                    oi_history.extend(oi)
                    cur_ts = oi[-1]['timestamp'] + (5 * 60 * 1000)
                    time.sleep(0.1)
                else:
                    break # Not supported
            except Exception as e:
                # Print once
                # print(f"  OI Error: {e}")
                break
                
        if oi_history:
            df_oi = pd.DataFrame(oi_history)
            df_oi.drop_duplicates(subset=['timestamp'], inplace=True)
            df_oi.to_csv(f"{out_path}/oi_history.csv", index=False)
            print(f"  ✅ Saved {len(df_oi)} OI records.")
        else:
            print("  ⚠️ OI History not supported or failed to fetch for this pair.")

if __name__ == "__main__":
    fetch_long_term_data()
