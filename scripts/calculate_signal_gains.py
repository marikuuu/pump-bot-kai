import pandas as pd
import json
import os
from datetime import datetime, timedelta

OHLCV_DIR = 'data/history/multiverse'

def calculate_gains():
    # Load the 33 detected hits
    with open('training_hits_list.json', 'r') as f:
        hits = json.load(f)
        
    results = []
    print(f"Calculating gains for {len(hits)} signals...")
    
    for hit in hits:
        sym = hit['symbol']
        jst_str = hit['jst']
        # Convert JST back to UTC TS (ms)
        jst_dt = datetime.strptime(jst_str, '%Y-%m-%d %H:%M:%S')
        utc_dt = jst_dt - timedelta(hours=9)
        start_ts = int(utc_dt.timestamp() * 1000)
        
        # Match OHLCV
        clean_sym = sym.replace('/', '_').replace(':', '_')
        ohlcv_path = os.path.join(OHLCV_DIR, f"{clean_sym}_1m.csv")
        
        if os.path.exists(ohlcv_path):
            try:
                df = pd.read_csv(ohlcv_path)
                # 3-day window (4320 mins)
                future = df[df['timestamp'] >= start_ts].head(4320)
                if not future.empty:
                    entry = future['open'].iloc[0]
                    peak = future['close'].max()
                    gain_pct = (peak - entry) / entry * 100
                    
                    results.append({
                        'symbol': sym,
                        'jst': jst_str,
                        'gain': f"+{gain_pct:.1f}%"
                    })
            except: continue
            
    # Output to a readable format
    print("\n--- PERFORMANCE SUMMARY (MAX % INCREASE IN 3 DAYS) ---")
    print(f"{'SYMBOL':<20} | {'DETECTION (JST)':<20} | {'MAX GAIN':<10}")
    print("-" * 55)
    for r in results:
        print(f"{r['symbol']:<20} | {r['jst']:<20} | {r['gain']:<10}")

if __name__ == '__main__':
    calculate_gains()
