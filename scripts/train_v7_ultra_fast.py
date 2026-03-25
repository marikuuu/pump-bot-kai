import pandas as pd
import numpy as np
import os
import json
from collections import defaultdict

TICK_DIR = 'data/history/multiverse/ticks'
OHLCV_DIR = 'data/history/multiverse'

def extract_ultra():
    with open('multiverse_candidates.json', 'r') as f:
        candidates = json.load(f)
    # candidates.sort(key=lambda x: x['start_ts'])
    
    ticks = os.listdir(TICK_DIR)
    tick_index = defaultdict(list)
    for t in ticks:
        if not t.endswith('_ticks.csv'): continue
        base = t.split('_')[0].lower()
        tick_index[base].append(t)
        
    results = []
    ohlcv_cache = {}
    tick_cache = {}
    
    print(f"Processing {len(candidates)} candidates with ULTRA caching...")
    
    # Pre-parse candidate timestamps
    for i, c in enumerate(candidates):
        if i % 2000 == 0: print(f"Processing {i}/{len(candidates)}...")
        
        sym = c.get('symbol', 'UNKNOWN')
        ts = int(c.get('start_ts', 0))
        base = sym.split('/')[0].lower()
        clean_sym = sym.replace('/', '_').replace(':', '_')
        
        # 1. Faster Tick Match
        tick_file = None
        best_diff = 4 * 60 * 60 * 1000
        for tf in tick_index[base]:
            # Extract TS from filename more efficiently
            if "_ticks.csv" in tf:
                try:
                    # Assumption: Filename has _177..._ticks.csv
                    f_ts = int(tf.split('_')[-2])
                    diff = abs(f_ts - ts)
                    if diff < best_diff:
                        best_diff = diff
                        tick_file = os.path.join(TICK_DIR, tf)
                except: continue

        if not tick_file: continue
        
        ohlcv_path = os.path.join(OHLCV_DIR, f"{clean_sym}_1m.csv")
        if not os.path.exists(ohlcv_path): continue
        
        try:
            if ohlcv_path not in ohlcv_cache:
                df = pd.read_csv(ohlcv_path)
                # Set index to timestamp for O(1) lookup
                df.set_index('timestamp', inplace=True)
                ohlcv_cache[ohlcv_path] = df
            
            df_ohlcv = ohlcv_cache[ohlcv_path]
            
            if tick_file not in tick_cache:
                tick_cache[tick_file] = pd.read_csv(tick_file)
            trades = tick_cache[tick_file]
            
            # 3. Features (Fast)
            # Find row index in OHLCV
            try:
                # Find nearest timestamp in index
                idx = df_ohlcv.index.get_indexer([ts], method='nearest')[0]
                if idx < 0: continue
                
                # Past 180 mins
                past = df_ohlcv.iloc[max(0, idx-180) : idx+1]
                if len(past) < 10: mom = 0
                else: mom = (past['close'].iloc[-1] - past['open'].iloc[0]) / past['open'].iloc[0]
                
                # Target
                future = df_ohlcv.iloc[idx : idx+4320]
                entry = future['open'].iloc[0]
                peak = future['close'].max()
                outcome = 1 if (peak / entry) >= 1.5 else 0
                
                amt_col = 'qty' if 'qty' in trades.columns else ('amount' if 'amount' in trades.columns else 'price')
                avg_size = trades[amt_col].mean() if amt_col in trades.columns else 0
                
                results.append({
                    'timestamp': ts,
                    'volume_z': c.get('peak_vol_z', c.get('vol_z', 0.0)),
                    'price_z': c.get('peak_pc_z', c.get('pc_z', 0.0)),
                    'avg_trade_size': avg_size,
                    'momentum': mom,
                    'target': outcome
                })
            except: continue

            # Cache Limit
            if len(ohlcv_cache) > 30: ohlcv_cache.pop(next(iter(ohlcv_cache)))
            if len(tick_cache) > 50: tick_cache.pop(next(iter(tick_cache)))
            
        except: continue
        
    final_df = pd.DataFrame(results)
    final_df.to_csv('data/ml_dataset_v4.csv', index=False)
    print(f"Saved {len(final_df)} samples. Positive Targets: {final_df['target'].sum()}")

if __name__ == '__main__':
    extract_ultra()
