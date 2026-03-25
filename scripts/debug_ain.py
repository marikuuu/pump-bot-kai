import pandas as pd
from datetime import datetime, timedelta
import os
import json

def debug_ain():
    # 1. Load OHLCV
    ohlcv_path = 'data/history/multiverse/AIN_USDT_USDT_1m.csv'
    df = pd.read_csv(ohlcv_path)
    df['jst'] = df['timestamp'].apply(lambda ts: datetime.fromtimestamp(ts/1000) + timedelta(hours=9))
    
    # 2. Find the major 2x pump
    df['max_3d_future'] = df['close'].rolling(window=288*3).max().shift(-288*3)
    df['pump_1.5x'] = df['max_3d_future'] / df['close'] >= 1.5
    
    true_pumps = df[df['pump_1.5x']]
    if not true_pumps.empty:
        print("--- AIN Ground Truth (1.5x in 3 days) ---")
        # Show first 5 and last 5 of true pump periods
        print(true_pumps[['jst', 'close']].iloc[[0, -1]])
        
    # 3. Load Detections
    ml_df = pd.read_csv('data/ml_dataset_v4.csv')
    ain_ml = ml_df[ml_df['target'] == 1].copy() # This isn't right because symbol was lost.
    # Re-extract based on timestamp from candidates.json
    with open('multiverse_candidates.json', 'r') as f:
        cand = json.load(f)
    ain_cands = [c for c in cand if 'AIN' in c['symbol']]
    
    print("\n--- AIN Candidate Triggers (Multiverse) ---")
    for c in ain_cands:
        ts = int(c['start_ts'])
        jst = datetime.fromtimestamp(ts/1000) + timedelta(hours=9)
        # Check if this candidate is in our ML dataset and has target=1
        match = ml_df[abs(ml_df['timestamp'] - ts) < 1000]
        if not match.empty and match.iloc[0]['target'] == 1:
            print(f"TRIGGER DETECTED: {jst} | Close: {c['close']} | Target: 1")
        else:
            status = "Target: 0" if not match.empty else "No Feature Extracted"
            print(f"Candidate: {jst} | Close: {c['close']} | {status}")

if __name__ == '__main__':
    debug_ain()
