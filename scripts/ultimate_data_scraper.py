import ccxt
import pandas as pd
import os
import time
from datetime import datetime, timedelta

def load_targets():
    df = pd.read_csv('5x_pumpers_list_futures.csv')
    targets = ['XGZ/USDT', 'COAI/USDT', 'SIREN/USDT:USDT', 'POWER/USDT']
    filtered = df[df['symbol'].isin(targets)].copy()
    
    # In case POWER was on spot or didn't get caught in the futures CSV
    if 'POWER/USDT' not in filtered['symbol'].values:
        # Add POWER manually from 5x spot scan
        filtered = pd.concat([filtered, pd.DataFrame([{
            'symbol': 'POWER/USDT', 'exchange': 'mexc',
            'bottom_date': '2025-12-05', 'peak_date': '2026-03-02'
        }])], ignore_index=True)
        
    return filtered

def fetch_history_forward(ex, symbol, out_dir, bottom_ts, peak_ts):
    print(f"\\n--- 🚀 Initiating Ultimate Scrape for {symbol} ---")
    start_dt = datetime.fromtimestamp(bottom_ts/1000)
    end_dt = datetime.fromtimestamp(peak_ts/1000)
    print(f"    Target Window: {start_dt} to {end_dt}")
    
    # 1. Fetch OHLCV 1m
    print(f" [1/3] Downloading 1m OHLCV...")
    all_ohlcv = []
    cur_ts = bottom_ts - (24 * 3600 * 1000) # 1 day buffer
    while cur_ts < peak_ts + (24 * 3600 * 1000):
        try:
            ohlcv = ex.fetch_ohlcv(symbol, timeframe='1m', since=cur_ts, limit=1000)
            if not ohlcv: break
            all_ohlcv.extend(ohlcv)
            # Advance safely
            last_ts = ohlcv[-1][0]
            if last_ts <= cur_ts: break
            cur_ts = last_ts + 60000
            time.sleep(0.1)
        except Exception as e:
            print(f"       ERROR (OHLCV): {e}")
            time.sleep(1)
            
    if all_ohlcv:
        df_k = pd.DataFrame(all_ohlcv, columns=['ts','o','h','l','c','v'])
        df_k.drop_duplicates(subset=['ts'], inplace=True)
        df_k.to_csv(f"{out_dir}/ohlcv_1m.csv", index=False)
        print(f"       ✅ Saved {len(df_k)} 1m candles.")
        
    # 2. Fetch Open Interest
    print(f" [2/3] Downloading Open Interest (OI) History...")
    oi_history = []
    cur_ts = bottom_ts - (24 * 3600 * 1000)
    oi_supported = 'fetch_open_interest_history' in ex.has and ex.has['fetch_open_interest_history']
    
    if oi_supported:
        while cur_ts < peak_ts + (24 * 3600 * 1000):
            try:
                oi = ex.fetch_open_interest_history(symbol, timeframe='5m', since=cur_ts, limit=500)
                if not oi: break
                oi_history.extend(oi)
                last_ts = oi[-1]['timestamp']
                if last_ts <= cur_ts: break
                cur_ts = last_ts + 300000
                time.sleep(0.2)
            except Exception as e:
                # Sometimes just unsupported for this market remotely
                break
                
    if oi_history:
        df_oi = pd.DataFrame(oi_history)
        df_oi.drop_duplicates(subset=['timestamp'], inplace=True)
        df_oi.to_csv(f"{out_dir}/oi_history.csv", index=False)
        print(f"       ✅ Saved {len(df_oi)} OI records.")
    else:
        print(f"       ⚠️ OI Not available/supported for {symbol}.")
        
    # 3. Fetch Tick Trades (Extremely aggressive pagination)
    print(f" [3/3] Downloading TICK Level Data (Trades)...")
    all_trades = []
    cur_ts = bottom_ts - (12 * 3600 * 1000) # 12 hr buffer before bottom
    
    no_progress_count = 0
    while cur_ts < peak_ts:
        try:
            trades = ex.fetch_trades(symbol, since=cur_ts, limit=1000, params={'until': int(cur_ts + (3600 * 1000))})
            if not trades:
                cur_ts += 3600 * 1000
                time.sleep(0.05)
                continue
                
            all_trades.extend(trades)
            last_trade_ts = trades[-1]['timestamp']
            
            if last_trade_ts <= cur_ts:
                # API is returning the same window (e.g., maximum history limit hit)
                no_progress_count += 1
                if no_progress_count > 3:
                     # Stop infinite looping and hitting max history ceiling
                     print(f"       ⚠️ Tick API history limit reached. Server won't rewind further.")
                     break
                cur_ts += 1 # bump just in case
            else:
                cur_ts = last_trade_ts + 1
                no_progress_count = 0
                
            if len(all_trades) % 50000 == 0:
                print(f"       ...{len(all_trades)} trades fetched. Reached {datetime.fromtimestamp(cur_ts/1000)}")
            
            time.sleep(0.05) # Rate limit respect
            
        except Exception as e:
            if "not support" in str(e).lower() or "not exist" in str(e).lower() or "out of bounds" in str(e).lower():
                 print(f"       ⚠️ Tick History limited by exchange rules: {e}")
                 break
            print(f"       Tick Fetch Error: {e}, Retrying...")
            time.sleep(2)
            
    if all_trades:
        # Convert to strict CSV dataframe
        df_t = pd.DataFrame(all_trades)
        if 'info' in df_t.columns: df_t.drop('info', axis=1, inplace=True)
        df_t.drop_duplicates(subset=['id'], inplace=True)
        df_t.to_csv(f"{out_dir}/trades_tick.csv", index=False)
        print(f"       ✅ 🔥 Saved {len(df_t)} pure tick records.")
    else:
        print(f"       ❌ Failed to fetch tick records or too old.")

def main():
    mexc_swap = ccxt.mexc({'options': {'defaultType': 'swap'}})
    mexc_spot = ccxt.mexc({'options': {'defaultType': 'spot'}})
    
    mexc_swap.load_markets()
    mexc_spot.load_markets()
    
    targets_df = load_targets()
    base_dir = "data/ultimate_dna"
    os.makedirs(base_dir, exist_ok=True)
    
    for _, row in targets_df.iterrows():
        symbol = row['symbol']
        b_date = pd.to_datetime(row['bottom_date'])
        p_date = pd.to_datetime(row['peak_date'])
        
        # Determine milliseconds
        bottom_ts = int(b_date.timestamp() * 1000)
        peak_ts = int(p_date.timestamp() * 1000)
        
        # Find which exchange instance to use
        ex = mexc_swap
        if symbol not in ex.symbols:
             ex = mexc_spot
             if symbol not in ex.symbols:
                  print(f"⚠️ {symbol} not found on MEXC Spot or Swap.")
                  continue
                  
        out_path = os.path.join(base_dir, symbol.replace('/','_').replace(':','_'))
        os.makedirs(out_path, exist_ok=True)
        
        fetch_history_forward(ex, symbol, out_path, bottom_ts, peak_ts)

if __name__ == "__main__":
    main()
