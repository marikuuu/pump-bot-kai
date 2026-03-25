import pandas as pd
import json
from datetime import datetime, timedelta

def list_detections():
    df = pd.read_csv('data/ml_dataset_v4.csv')
    pumps = df[df['target'] == 1].copy()
    
    with open('multiverse_candidates.json', 'r') as f:
        cand = json.load(f)

    print('--- Verified Pumps Detected (v4.0 Ensemble) ---')
    print(f"{'Symbol':<20} | {'Time (JST)':<20} | {'Momentum'}")
    print("-" * 60)
    
    found_symbols = set()
    for i, row in pumps.iterrows():
        ts = int(row['timestamp'])
        # Find closest symbol in candidates
        match = [c['symbol'] for c in cand if abs(c['start_ts'] - ts) < 2 * 60 * 60 * 1000]
        if match:
            sym = match[0]
            if sym in found_symbols: continue # Only list first trigger per pump
            jst = datetime.fromtimestamp(ts / 1000) + timedelta(hours=9)
            print(f"{sym:<20} | {jst.strftime('%Y-%m-%d %H:%M'):<20} | {row['price_momentum_3h']:>8.2f}")
            found_symbols.add(sym)

if __name__ == '__main__':
    list_detections()
