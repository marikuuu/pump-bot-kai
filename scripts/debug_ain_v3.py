import pandas as pd
from datetime import datetime, timedelta
import os
import json

def debug_ain():
    # 1. Load OHLCV
    ohlcv_path = 'data/history/multiverse/AIN_USDT_USDT_1m.csv'
    df = pd.read_csv(ohlcv_path)
    df['jst'] = df['timestamp'].apply(lambda ts: datetime.fromtimestamp(ts/1000) + timedelta(hours=9))
    
    # 2. Load Detections from ML Dataset
    ml_path = 'data/ml_dataset_v4.csv'
    if os.path.exists(ml_path):
        ml_df = pd.read_csv(ml_path)
    else:
        print("ML dataset not found!")
        return
    
    # 3. Load Candidates
    with open('multiverse_candidates.json', 'r') as f:
        cand = json.load(f)
    
    print("\n--- AIN Signal Timeline (JST) ---")
    signals = []
    for c in cand:
        if 'AIN' in c['symbol']:
            ts = int(c['start_ts'])
            jst = datetime.fromtimestamp(ts/1000) + timedelta(hours=9)
            # Find in ML dataset
            match = ml_df[abs(ml_df['timestamp'] - ts) < 1000]
            if not match.empty:
                is_pump = match.iloc[0]['target'] == 1
                signals.append({'jst': jst, 'target': is_pump})
    
    if signals:
        sig_df = pd.DataFrame(signals).sort_values('jst')
        print(sig_df[sig_df['target'] == True])
    else:
        print("No AIN signals found in candidate list.")
    
    print("\n--- AIN Price History (Past 2 Days to Now) ---")
    now_jst = datetime(2026, 3, 23, 10, 4)
    history = df[(df['jst'] >= now_jst - timedelta(days=2)) & (df['jst'] <= now_jst)]
    if not history.empty:
        # Just show start, middle, end
        print(history.iloc[[0, len(history)//2, -1]][['jst', 'close']])
        
    print("\n--- Verification of 2x Pump ---")
    max_price = history['close'].max()
    min_price = history['close'].min()
    print(f"Min: {min_price:.4f} | Max: {max_price:.4f} | Gain: {max_price/min_price:.2f}x")

if __name__ == '__main__':
    debug_ain()
