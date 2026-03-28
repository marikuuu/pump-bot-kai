import ccxt
import pandas as pd
import os
import time
from datetime import datetime, timedelta

def save_complete_naked_data():
    binance = ccxt.binance({'options': {'defaultType': 'future'}})
    mexc = ccxt.mexc({'options': {'defaultType': 'swap'}})
    
    # 1. Load the discovery list
    if not os.path.exists("golden_pumps_list.csv"):
        print("Error: golden_pumps_list.csv not found.")
        return
    
    df_golden = pd.read_csv("golden_pumps_list.csv")
    
    # 2. Filter for "Golden" candidates (Gain > 1.7x for high quality)
    targets = df_golden[df_golden['gain_x'] >= 1.7].head(20)
    
    print(f"🚀 Starting Bulk Data Save for {len(targets)} targets (including OI & Funding)...")
    
    base_dir = "data/naked_dna"
    os.makedirs(base_dir, exist_ok=True)

    for _, row in targets.iterrows():
        symbol = row['symbol']
        pump_end_ts = int(row['timestamp'])
        start_ts = pump_end_ts - (7 * 24 * 60 * 60 * 1000)
        end_ts = pump_end_ts + (6 * 60 * 60 * 1000)
        
        # Determine exchange (Binance preference)
        ex = binance
        try:
            ex.load_markets()
            if symbol not in ex.symbols:
                if f"{symbol}:USDT" in ex.symbols: symbol = f"{symbol}:USDT"
                else: ex = mexc; ex.load_markets() # Fallback to MEXC
        except: ex = mexc; ex.load_markets()

        out_path = os.path.join(base_dir, f"{symbol.replace('/', '_').replace(':', '_')}")
        os.makedirs(out_path, exist_ok=True)

        print(f"--- Processing {symbol} on {ex.id} ---")

        # A. Fetch 1m OHLCV
        try:
            all_ohlcv = []
            cur_ts = start_ts
            while cur_ts < end_ts:
                ohlcv = ex.fetch_ohlcv(symbol, timeframe='1m', since=cur_ts, limit=1000)
                if not ohlcv: break
                all_ohlcv.extend(ohlcv)
                cur_ts = ohlcv[-1][0] + 60000
            pd.DataFrame(all_ohlcv, columns=['ts','o','h','l','c','v']).to_csv(f"{out_path}/ohlcv_1m.csv", index=False)
        except Exception as e: print(f"  OHLCV Error: {e}")

        # B. Fetch Funding Rate History
        try:
            funding = ex.fetch_funding_rate_history(symbol, since=start_ts)
            pd.DataFrame(funding).to_csv(f"{out_path}/funding_history.csv", index=False)
        except Exception as e: print(f"  Funding Error: {e}")

        # C. Fetch Open Interest (OI) History - 🚀 CRITICAL
        try:
            # Note: CCXT fetch_open_interest_history often needs specific timeframes or handles differently per ex
            # For Binance Futures, we can often get historical OI
            oi_history = []
            cur_ts = start_ts
            while cur_ts < end_ts:
                # Binance specific historical OI API via fetch_ohlcv if type is 'open_interest' 
                # OR direct fetch if supported.
                try:
                    # Generic fetch_open_interest_history
                    oi = ex.fetch_open_interest_history(symbol, timeframe='5m', since=cur_ts, limit=500)
                    if not oi: break
                    oi_history.extend(oi)
                    cur_ts = oi[-1]['timestamp'] + 300000
                except: break
            
            if oi_history:
                pd.DataFrame(oi_history).to_csv(f"{out_path}/oi_history.csv", index=False)
        except Exception as e: print(f"  OI Error: {e}")

        print(f"  ✅ Saved data to {out_path}")
        time.sleep(1) # Rate limiting care

if __name__ == "__main__":
    save_complete_naked_data()
