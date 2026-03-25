import pandas as pd
from datetime import datetime, timedelta
import os
import json

def long_term_debug():
    # 1. Load OHLCV
    ohlcv_path = 'data/history/multiverse/AIN_USDT_USDT_1m.csv'
    df = pd.read_csv(ohlcv_path)
    df['jst'] = df['timestamp'].apply(lambda ts: datetime.fromtimestamp(ts/1000))
    
    print("--- AIN Long Term Price Action (3/16 - 3/23) ---")
    # Identify price 8 days ago
    start_time = df['jst'].min()
    end_time = df['jst'].max()
    print(f"Data Range: {start_time} to {end_time}")
    
    # Identify the huge pumps (>1.5x in 3 days)
    # We'll use a 3-day window
    df['max_3d'] = df['close'].rolling(window=288*3).max().shift(-288*3)
    pumps = df[df['max_3d'] / df['close'] >= 1.5].copy()
    
    if not pumps.empty:
        print("\n--- ALL True Pump Opportunities (1.5x in 3d) ---")
        # Just show the start of each distinct pump period
        pumps['diff'] = pumps['jst'].diff() > timedelta(hours=12)
        distinct_pumps = pumps[pumps['diff'] | (pumps['diff'].shift(-1).isna())]
        print(distinct_pumps[['jst', 'close']])
    else:
        print("\nNo 1.5x pump opportunities found in the raw data files.")

    # 4. Load Candidates and check triggers
    with open('multiverse_candidates.json', 'r') as f:
        cand = json.load(f)
    
    ml_df = pd.read_csv('data/ml_dataset_v4.csv')
    
    print("\n--- ALL AIN Ensemble Signals (Target=1) ---")
    for c in cand:
        if 'AIN' in c['symbol']:
            ts = int(c['start_ts'])
            jst = datetime.fromtimestamp(ts/1000)
            match = ml_df[abs(ml_df['timestamp'] - ts) < 1000]
            if not match.empty and match.iloc[0]['target'] == 1:
                print(f"SIGNAL: {jst} | vol_z: {c.get('vol_z'):.2f} | std_rush: {c.get('std_rush'):.2f}")

if __name__ == '__main__':
    long_term_debug()
