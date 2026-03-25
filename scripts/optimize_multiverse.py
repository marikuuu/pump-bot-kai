import pandas as pd
import numpy as np
import os
import json
import itertools
from concurrent.futures import ProcessPoolExecutor
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def process_event(c):
    tick_dir = 'data/history/multiverse/ticks'
    ohlcv_dir = 'data/history/multiverse'
    
    symbol = c['symbol']
    start_ts = c['start_ts']
    clean = symbol.replace('/', '_').replace(':', '_')
    tick_file = os.path.join(tick_dir, f"{clean}_{start_ts}_ticks.csv")
    
    if not os.path.exists(tick_file):
        return None
        
    try:
        trades = pd.read_csv(tick_file)
        if trades.empty: return None
        
        # Calculate rush
        trades['ts_dt'] = pd.to_datetime(trades['timestamp'], unit='ms')
        trades['sec'] = trades['ts_dt'].dt.second
        rush = trades.groupby('sec').size().reindex(range(0, 60), fill_value=0).std()
        
        # Check outcome (Target: 1.5x gain (+50%) in the next 3 days)
        ohlcv_file = os.path.join(ohlcv_dir, f"{clean}_1m.csv")
        outcome = False
        max_gain = 0.0
        if os.path.exists(ohlcv_file):
            df = pd.read_csv(ohlcv_file)
            # 3 days = 3 * 24 * 60 = 4320 minutes
            future = df[df['timestamp'] >= start_ts].head(4320)
            if not future.empty:
                start_price = future['open'].iloc[0]
                max_price = future['high'].max()
                max_gain = (max_price - start_price) / start_price
                outcome = max_gain >= 0.50 # 1.5x gain
                
        return {
            'symbol': symbol,
            'start_ts': start_ts,
            'vol_z': c['peak_vol_z'],
            'pc_z': c['peak_pc_z'],
            'rush': rush,
            'outcome': outcome,
            'max_gain': max_gain
        }
    except Exception as e:
        return None

def main():
    candidates_file = 'multiverse_candidates.json'
    if not os.path.exists(candidates_file):
        print("No candidates found.")
        return
        
    with open(candidates_file, 'r') as f:
        candidates = json.load(f)
        
    print(f"Loading data & calculating features for {len(candidates)} events...")
    
    # Process all events once and cache in memory
    results = []
    with ProcessPoolExecutor() as executor:
        for res in executor.map(process_event, candidates):
            if res is not None:
                results.append(res)
                
    if not results:
        print("No tick data loaded.")
        return
        
    df = pd.DataFrame(results)
    print(f"Successfully loaded {len(df)} valid events.")
    
    # Grid Search
    grid = {
        'std_rush': [3.0, 4.0, 5.0, 6.0, 8.0, 10.0],
        'vol_z': [2.0, 3.0, 4.0, 5.0],
        'pc_z': [1.5, 2.0, 2.5]
    }
    
    keys = grid.keys()
    combinations = list(itertools.product(*grid.values()))
    
    print(f"Testing {len(combinations)} threshold combinations...")
    summary = []
    
    for combo in combinations:
        params = dict(zip(keys, combo))
        
        # Vectorized condition check (Stage 2 + Stage 3 rules)
        # Stage 2: vol_z > VOL_Z_THRESH and pc_z > PC_Z_THRESH
        # Stage 3 rule fallback: std_rush > RUSH_THRESH and vol_z > VOL_Z
        
        signals = df[
            (df['vol_z'] > params['vol_z']) & 
            (df['pc_z'] > params['pc_z']) & 
            (df['rush'] > params['std_rush'])
        ]
        
        tp = len(signals[signals['outcome'] == True])
        fp = len(signals[signals['outcome'] == False])
        total_signals = tp + fp
        
        precision = tp / total_signals if total_signals > 0 else 0
        fn = len(df[df['outcome'] == True]) - tp # Note: df['outcome'] == True are all true pumps in the dataset
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        
        summary.append({
            'std_rush': params['std_rush'],
            'vol_z': params['vol_z'],
            'pc_z': params['pc_z'],
            'signals': total_signals,
            'TP': tp,
            'FP': fp,
            'precision': precision,
            'recall': recall
        })

    viz_df = pd.DataFrame(summary)
    winners = viz_df[(viz_df['TP'] > 0) & (viz_df['precision'] >= 0.80)].sort_values('TP', ascending=False)
    
    print("\n--- Top Thresholds (Precision >= 80% & TP > 0) ---")
    if not winners.empty:
        print(winners.head(15).to_string())
    else:
        print("No combination reached 80% precision. Let's look at best precision > 50%:")
        ok = viz_df[(viz_df['TP'] > 0) & (viz_df['precision'] >= 0.50)].sort_values('TP', ascending=False)
        print(ok.head(15).to_string() if not ok.empty else "Still no signals. Data might not contain enough true pumps.")
        
    viz_df.to_csv('multiverse_optimization_results.csv', index=False)
    print("Saved all results to multiverse_optimization_results.csv")

if __name__ == "__main__":
    main()
