import pandas as pd
import json
from datetime import datetime, timedelta
import os

def final_report():
    df_ml = pd.read_csv('data/ml_dataset_v4.csv')
    with open('multiverse_candidates.json', 'r') as f:
        cand = json.load(f)
    
    print("--- COMPLETE Signal History (v4.0 Ensemble) ---")
    print(f"{'Symbol':<20} | {'Trigger Time (JST)':<20} | {'Max Gain (Next 3d)'}")
    print("-" * 70)
    
    positives = df_ml[df_ml['target'] == 1].copy()
    
    for i, row in positives.iterrows():
        ts = int(row['timestamp'])
        jst = datetime.fromtimestamp(ts/1000)
        
        match = [c['symbol'] for c in cand if abs(c['start_ts'] - ts) < 1000]
        if match:
            sym = match[0]
            f_path = f"data/history/multiverse/{sym.replace('/', '_').replace(':', '_')}_1m.csv"
            if not os.path.exists(f_path): continue
            
            df_ohlcv = pd.read_csv(f_path)
            future = df_ohlcv[df_ohlcv['timestamp'] >= ts].head(4320)
            if not future.empty:
                entry = future['open'].iloc[0]
                max_p = future['close'].max()
                gain = max_p / entry
                print(f"{sym:<20} | {jst.strftime('%m/%d %H:%M'):<20} | {gain:.2f}x")

if __name__ == '__main__':
    final_report()
