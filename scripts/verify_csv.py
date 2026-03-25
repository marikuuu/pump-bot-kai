import pandas as pd
import json
from datetime import datetime
import os

def verify():
    df = pd.read_csv('data/ml_dataset_v4.csv')
    with open('multiverse_candidates.json', 'r') as f:
        cand = json.load(f)
    
    print("--- Audit of 11 Positive Signals found in CSV ---")
    positives = df[df['target'] == 1].copy()
    
    for i, row in positives.iterrows():
        ts = row['timestamp']
        jst = datetime.fromtimestamp(ts/1000)
        # Find symbol
        match = [c['symbol'] for c in cand if abs(c['start_ts'] - ts) < 1000]
        if match:
            sym = match[0]
            print(f"SIGNAL: {sym:<20} | Time: {jst} | Target=1")

if __name__ == '__main__':
    verify()
