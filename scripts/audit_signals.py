import pandas as pd
import json
from datetime import datetime, timedelta
import os

def audit():
    df_ml = pd.read_csv('data/ml_dataset_v4.csv')
    with open('multiverse_candidates.json', 'r') as f:
        cand = json.load(f)
    
    positives = df_ml[df_ml['target'] == 1].head(10)
    
    print("--- Detailed Audit of Top 10 Positive Signals ---")
    for i, row in positives.iterrows():
        ts = int(row['timestamp'])
        jst = datetime.fromtimestamp(ts/1000)
        
        match = [c['symbol'] for c in cand if abs(c['start_ts'] - ts) < 1000]
        if match:
            sym = match[0]
            # Load the OHLCV for this symbol
            f_path = f"data/history/multiverse/{sym.replace('/', '_').replace(':', '_')}_1m.csv"
            if not os.path.exists(f_path): continue
            
            df_ohlcv = pd.read_csv(f_path)
            future = df_ohlcv[df_ohlcv['timestamp'] >= ts].head(4320)
            if not future.empty:
                entry = future['open'].iloc[0]
                max_p = future['close'].max()
                gain = max_p / entry
                print(f"{sym:<20} | {jst} | Entry: {entry:.4f} | Future Max: {max_p:.4f} | Gain: {gain:.2f}x")

if __name__ == '__main__':
    audit()
