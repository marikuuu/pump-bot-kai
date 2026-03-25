import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timezone

# ─── 定数 ──────────────────────────────────────────
START_TS = int(datetime(2026, 3, 15, 15, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
END_TS   = int(datetime(2026, 3, 21, 15, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
DATA_DIR = "data/history/multiverse"
BTC_FILE = os.path.join(DATA_DIR, "BTC_USDT_USDT_1m.csv")

def precalculate_points(v_th, p_th):
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*_USDT_USDT_1m.csv")))
    files = [f for f in files if '260327' not in f and '260626' not in f]
    
    df_btc = pd.read_csv(BTC_FILE)
    df_btc['pc_15m'] = df_btc['close'].pct_change(15)
    btc_map = {row['timestamp']: row['pc_15m'] for _, row in df_btc.iterrows()}
    
    all_gains = []
    for f in files:
        symbol = os.path.basename(f).replace("_USDT_USDT_1m.csv", "")
        if symbol == "BTC": continue
        try:
            df = pd.read_csv(f)
            df['vol_z'] = (df['volume'].rolling(30).mean() - df['volume'].rolling(100).mean().shift(30)) / (df['volume'].rolling(100).std().shift(30) + 1e-9)
            df['pc'] = df['close'].pct_change()
            df['pc_z'] = (df['pc'].rolling(30).mean() - df['pc'].rolling(100).mean().shift(30)) / (df['pc'].rolling(100).std().shift(30) + 1e-9)
            
            raw_df = pd.read_csv(f)
            raw_df['high_future'] = raw_df['high'].rolling(4320).max().shift(-4320)
            df['max_gain'] = (df['timestamp'].map(raw_df.set_index('timestamp')['high_future']) / df['close'] - 1.0) * 100
            df['btc_crash'] = df['timestamp'].map(btc_map)
            
            # Detect
            candidates = df[(df['timestamp'] >= START_TS) & (df['timestamp'] <= END_TS) & 
                            (df['vol_z'] > v_th) & (df['pc_z'] > p_th) & (df['btc_crash'] >= -0.01)].copy()
            
            # Simple dedup: 1 hour cooldown
            candidates['dt'] = pd.to_datetime(candidates['timestamp'], unit='ms')
            candidates = candidates.sort_values('timestamp')
            last_ts = 0
            for _, row in candidates.iterrows():
                if row['timestamp'] - last_ts > 3600000:
                    all_gains.append(row['max_gain'])
                    last_ts = row['timestamp']
        except: continue
    return all_gains

if __name__ == "__main__":
    thresholds = [
        (3.0, 1.5),
        (4.0, 2.0),
        (5.0, 2.5),
        (6.0, 3.0)
    ]
    
    for v, p in thresholds:
        print(f"\n--- Testing Extreme Threshold: V={v}, P={p} ---")
        gains = precalculate_points(v, p)
        total = len(gains)
        if total == 0:
            print("No signals found at this level.")
            continue
            
        tp_20 = len([g for g in gains if g >= 20.0])
        precision = (tp_20 / total) * 100
        print(f"Total Signals: {total} | TP(20%+): {tp_20} | Precision: {precision:.1f}%")
