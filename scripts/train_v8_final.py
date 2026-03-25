import pandas as pd
import numpy as np
import os
import json
import time
from collections import defaultdict

TICK_DIR = 'data/history/multiverse/ticks'
OHLCV_DIR = 'data/history/multiverse'

def extract_timed():
    with open('multiverse_candidates.json', 'r') as f:
        candidates = json.load(f)
    
    ticks = os.listdir(TICK_DIR)
    tick_index = defaultdict(list)
    for t in ticks:
        if not t.endswith('_ticks.csv'): continue
        base = t.split('_')[0].lower()
        tick_index[base].append(t)
        
    results = []
    ohlcv_cache = {}
    tick_metrics_cache = {}
    
    print(f"Processing {len(candidates)} candidates with TIMING...")
    
    start_time = time.time()
    for i, c in enumerate(candidates): # Process all
        if i % 1000 == 0 and i > 0: 
            elapsed = time.time() - start_time
            print(f"Processing {i}/{len(candidates)}... Avg: {elapsed/i:.3f}s/cand")
        
        sym = c.get('symbol', 'UNKNOWN')
        ts = int(c.get('start_ts', 0))
        base = sym.split('/')[0].lower()
        
        # Match Tick
        tick_file = None
        best_diff = 4 * 60 * 60 * 1000
        for tf in tick_index[base]:
            try:
                # F-string or split might be slow?
                f_part = tf.split('_')[-2]
                if len(f_part) == 13:
                    diff = abs(int(f_part) - ts)
                    if diff < best_diff:
                        best_diff = diff
                        tick_file = os.path.join(TICK_DIR, tf)
            except: continue
        if not tick_file: continue
        
        # Match OHLCV
        clean_sym = sym.replace('/', '_').replace(':', '_')
        ohlcv_path = os.path.join(OHLCV_DIR, f"{clean_sym}_1m.csv")
        if not os.path.exists(ohlcv_path): continue
        
        try:
            if ohlcv_path not in ohlcv_cache:
                ohlcv_cache[ohlcv_path] = pd.read_csv(ohlcv_path).set_index('timestamp')
            df_ohlcv = ohlcv_cache[ohlcv_path]
            
            if tick_file not in tick_metrics_cache:
                t_df = pd.read_csv(tick_file)
                if t_df.empty: 
                    tick_metrics_cache[tick_file] = None
                else:
                    ts_col = 'timestamp' if 'timestamp' in t_df.columns else ('time' if 'time' in t_df.columns else 'T')
                    t_df['ts_dt'] = pd.to_datetime(t_df[ts_col], unit='ms')
                    t_df['sec'] = t_df['ts_dt'].dt.second
                    buyer_col = 'isBuyerMaker' if 'isBuyerMaker' in t_df.columns else 'is_buyer_maker'
                    amt_col = 'amount' if 'amount' in t_df.columns else 'qty'
                    
                    if buyer_col in t_df.columns:
                        buys = t_df[t_df[buyer_col] == False]
                        r_std = buys.groupby('sec').size().reindex(range(60), fill_value=0).std()
                        t_rat = buys[amt_col].sum() / t_df[amt_col].sum()
                    else:
                        r_std = t_df.groupby('sec').size().reindex(range(60), fill_value=0).std()
                        t_rat = 0.5
                        
                    tick_metrics_cache[tick_file] = {
                        'r_std': r_std, 't_rat': t_rat,
                        'avg': t_df[amt_col].mean(), 'max': t_df[amt_col].max()
                    }
                    
            m = tick_metrics_cache[tick_file]
            if not m: continue
            
            # OHLCV Features
            idx_list = df_ohlcv.index.get_indexer([ts], method='nearest')
            if not len(idx_list): continue
            idx = idx_list[0]
            
            # Momentum
            past = df_ohlcv.iloc[max(0, idx-180) : idx+1]
            mom = (past['close'].iloc[-1] - past['open'].iloc[0]) / past['open'].iloc[0] if len(past) > 10 else 0
            
            # Outcome
            future = df_ohlcv.iloc[idx : idx+4320]
            if future.empty: continue
            outcome = 1 if (future['close'].max() / future['open'].iloc[0]) >= 1.5 else 0
            
            results.append({
                'timestamp': ts, 'std_rush_orders': m['r_std'], 'taker_ratio': m['t_rat'],
                'avg_trade_size': m['avg'], 'max_trade_size': m['max'],
                'volume_z': c.get('peak_vol_z', c.get('vol_z', 0.0)),
                'price_z': c.get('peak_pc_z', c.get('pc_z', 0.0)),
                'momentum': mom,
                'target': outcome
            })
            
            if len(ohlcv_cache) > 20: ohlcv_cache.pop(next(iter(ohlcv_cache)))
            if len(tick_metrics_cache) > 50: tick_metrics_cache.pop(next(iter(tick_metrics_cache)))
        except: continue
        
    pd.DataFrame(results).to_csv('data/ml_dataset_v4.csv', index=False)
    print(f"Done. Saved {len(results)} samples.")

if __name__ == '__main__':
    extract_timed()
