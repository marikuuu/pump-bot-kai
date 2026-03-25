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
    
    # 3. Load Detections from ML Dataset
    ml_df = pd.read_csv('data/ml_dataset_v4.csv')
    
    # 4. Filter candidates.json for AIN and map to detections
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
                signals.append({'jst': jst, 'target': is_pump, 'vol_z': c['vol_z']})
    
    sig_df = pd.DataFrame(signals).sort_values('jst')
    print(sig_df)
    
    print("\n--- Current Price Status (3/23 10:04) ---")
    now_jst = datetime(2026, 3, 23, 10, 4)
    p_now = df[df['jst'] <= now_jst].tail(1)
    if not p_now.empty:
        print(f"Price at 10:04: {p_now.iloc[0]['close']}")
        
    print("\n--- Recent Historical Context (Past 24h) ---")
    yesterday_jst = now_jst - timedelta(hours=24)
    context = df[(df['jst'] >= yesterday_jst) & (df['jst'] <= now_jst)].iloc[::60] # Every hour
    print(context[['jst', 'close']])

if __name__ == '__main__':
    debug_ain()
