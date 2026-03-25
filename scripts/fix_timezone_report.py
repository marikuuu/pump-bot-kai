import pandas as pd
import json
from datetime import datetime
import os

def correct_report():
    df = pd.read_csv('data/ml_dataset_v4.csv')
    pumps = df[df['target'] == 1].copy()
    
    with open('multiverse_candidates.json', 'r') as f:
        cand = json.load(f)

    print('--- CORRECTED Verified Pumps (v4.0 Ensemble) ---')
    print(f"{'Symbol':<20} | {'Time (JST)':<20} | {'Status'}")
    print("-" * 60)
    
    now_jst = datetime(2026, 3, 23, 10, 4)
    found_symbols = set()
    for i, row in pumps.iterrows():
        ts = int(row['timestamp'])
        # NOTE: System is already JST, fromtimestamp gives JST.
        # DO NOT ADD 9 HOURS.
        jst = datetime.fromtimestamp(ts/1000)
        
        match = [c['symbol'] for c in cand if abs(c['start_ts'] - ts) < 1000]
        if match:
            sym = match[0]
            if sym in found_symbols: continue
            
            status = "Recent/Past" if jst < now_jst else "FUTURE (Simulated?)"
            print(f"{sym:<20} | {jst.strftime('%Y-%m-%d %H:%M'):<20} | {status}")
            found_symbols.add(sym)

if __name__ == '__main__':
    correct_report()
