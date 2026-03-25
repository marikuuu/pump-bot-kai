import pandas as pd
import os
import json
from datetime import datetime
from collections import defaultdict

TICK_DIR = 'data/history/multiverse/ticks'
OHLCV_DIR = 'data/history/multiverse'

def extract_fast():
    with open('multiverse_candidates.json', 'r') as f:
        candidates = json.load(f)
    candidates.sort(key=lambda x: x['start_ts'])
    
    ticks = os.listdir(TICK_DIR)
    tick_index = defaultdict(list)
    for t in ticks:
        if not t.endswith('_ticks.csv'): continue
        base = t.split('_')[0].lower()
        tick_index[base].append(t)
        
    results = []
    ohlcv_cache = {}
    tick_cache = {}
    
    print(f"Processing {len(candidates)} candidates with caching...")
    
    for i, c in enumerate(candidates):
        if i % 1000 == 0: print(f"Processing {i}/{len(candidates)}...")
        
        sym = c.get('symbol', 'UNKNOWN')
        ts = c.get('start_ts', 0)
        base = sym.split('/')[0].lower()
        
        # 1. Match Tick File
        tick_file = None
        best_diff = 4 * 60 * 60 * 1000
        for tf in tick_index[base]:
            parts = tf.split('_')
            for p in parts:
                if len(p) == 13 and p.isdigit():
                    diff = abs(int(p) - ts)
                    if diff < best_diff:
                        best_diff = diff
                        tick_file = os.path.join(TICK_DIR, tf)
            
        if not tick_file: continue
        
        # 2. Match OHLCV
        clean_sym = sym.replace('/', '_').replace(':', '_')
        ohlcv_path = os.path.join(OHLCV_DIR, f"{clean_sym}_1m.csv")
        if not os.path.exists(ohlcv_path): continue
        
        try:
            # Cache OHLCV
            if ohlcv_path not in ohlcv_cache:
                ohlcv_cache[ohlcv_path] = pd.read_csv(ohlcv_path)
            df_ohlcv = ohlcv_cache[ohlcv_path]
            
            # Cache Ticks
            if tick_file not in tick_cache:
                tick_cache[tick_file] = pd.read_csv(tick_file)
            trades = tick_cache[tick_file]
            
            # 3. Features
            vol_z = c.get('peak_vol_z', c.get('vol_z', 0.0))
            pc_z = c.get('peak_pc_z', c.get('pc_z', 0.0))
            
            amt_col = 'qty' if 'qty' in trades.columns else ('amount' if 'amount' in trades.columns else 'price')
            avg_size = trades[amt_col].mean() if amt_col in trades.columns else 0
            
            past = df_ohlcv[df_ohlcv['timestamp'] <= ts].tail(180)
            mom = (past['close'].iloc[-1] - past['open'].iloc[0]) / past['open'].iloc[0] if len(past) > 10 else 0
            
            # Target (Corrected localized 3d window)
            future = df_ohlcv[df_ohlcv['timestamp'] >= ts].head(4320)
            outcome = 0
            if not future.empty:
                entry = future['open'].iloc[0]
                peak = future['close'].max()
                if (peak / entry) >= 1.5: outcome = 1
                
            results.append({
                'timestamp': ts,
                'volume_z': vol_z,
                'price_z': pc_z,
                'avg_trade_size': avg_size,
                'momentum': mom,
                'target': outcome
            })
            
            # Keep cache small (FIFO or just clear periodically)
            if len(ohlcv_cache) > 20: ohlcv_cache.pop(next(iter(ohlcv_cache)))
            if len(tick_cache) > 50: tick_cache.pop(next(iter(tick_cache)))
            
        except: continue
        
    final_df = pd.DataFrame(results)
    final_df.to_csv('data/ml_dataset_v4.csv', index=False)
    print(f"Saved {len(final_df)} samples. Positive Targets: {final_df['target'].sum()}")

if __name__ == '__main__':
    extract_fast()
