import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timezone

# ─── 定数 ──────────────────────────────────────────
OUTCOME_DAYS = 3
SL_PENALTY   = 3.0
START_TS = int(datetime(2026, 3, 15, 15, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
END_TS   = int(datetime(2026, 3, 21, 15, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
DATA_DIR = "data/history/multiverse"
BTC_FILE = os.path.join(DATA_DIR, "BTC_USDT_USDT_1m.csv")

def load_btc_crash_map():
    if not os.path.exists(BTC_FILE): return {}
    df_btc = pd.read_csv(BTC_FILE)
    df_btc['pc_15m'] = df_btc['close'].pct_change(15)
    return {row['timestamp']: row['pc_15m'] for _, row in df_btc.iterrows()}

def precalculate_all_indicators():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*_USDT_USDT_1m.csv")))
    files = [f for f in files if '260327' not in f and '260626' not in f]
    
    btc_crashes = load_btc_crash_map()
    all_points = []
    
    print(f"🔄 Pre-calculating indicators for {len(files)} symbols...")
    for f in files:
        symbol = os.path.basename(f).replace("_USDT_USDT_1m.csv", "")
        if symbol == "BTC": continue
        try:
            df = pd.read_csv(f)
            # Add indicators
            df['vol_roll'] = df['volume'].rolling(30).mean()
            df['vol_std']  = df['volume'].rolling(100).std().shift(30)
            df['vol_mean'] = df['volume'].rolling(100).mean().shift(30)
            df['vol_z']    = (df['vol_roll'] - df['vol_mean']) / (df['vol_std'] + 1e-9)
            
            df['pc'] = df['close'].pct_change()
            df['pc_roll'] = df['pc'].rolling(30).mean()
            df['pc_std']  = df['pc'].rolling(100).std().shift(30)
            df['pc_mean'] = df['pc'].rolling(100).mean().shift(30)
            df['pc_z']    = (df['pc_roll'] - df['pc_mean']) / (df['pc_std'] + 1e-9)
            
            # Filter to test window
            df = df[(df['timestamp'] >= START_TS) & (df['timestamp'] <= END_TS)].copy()
            if df.empty: continue

            # Pre-calc outcome (Max High in next 3 days)
            # This bit is slow, but we only do it once
            # Optimization: since we do it for all rows, use rolling max
            # 3 days = 4320 minutes
            # BUT we need to load enough data to see the future
            raw_df = pd.read_csv(f)
            raw_df['high_future'] = raw_df['high'].rolling(4320).max().shift(-4320)
            
            # Map back to our test df
            df['max_future'] = df['timestamp'].map(raw_df.set_index('timestamp')['high_future'])
            df['max_gain']   = (df['max_future'] / df['close'] - 1.0) * 100
            df['btc_crash']  = df['timestamp'].map(btc_crashes)
            
            # Keep only candidate rows to save memory
            candidates = df[(df['vol_z'] > 1.0) & (df['pc_z'] > 0.5) & (df['btc_crash'] >= -0.01)]
            all_points.append(candidates[['vol_z', 'pc_z', 'max_gain']])
        except: continue
    
    return pd.concat(all_points)

if __name__ == "__main__":
    points = precalculate_all_indicators()
    print(f"✅ Pre-calculation done. Testing {len(points)} candidate points.")
    
    v_range = [1.5, 2.0, 2.5, 3.0]
    p_range = [1.0, 1.5, 2.0, 2.5]
    
    results = []
    for v in v_range:
        for p in p_range:
            matches = points[(points['vol_z'] > v) & (points['pc_z'] > p)]
            # Dedup signals (approximate by just taking a fraction or assuming they are spread)
            # In real backtest we have cooldown, but for optimization we can use matches.count() 
            # as a proxy for signal density.
            sig_count = len(matches) # This is 1-minute resolution signals
            
            # Heuristic: Real signal count is ~1/30 of minute-matches due to 1h cooldown
            real_sigs = sig_count // 10
            
            tp_matches = matches[matches['max_gain'] >= 50.0]
            real_tp = len(tp_matches) // 10 # Heuristic
            real_fp = real_sigs - real_tp
            
            score = (real_tp * 50.0) - (real_fp * SL_PENALTY)
            results.append({'v_z': v, 'p_z': p, 'sigs': real_sigs, 'tp': real_tp, 'score': score})
            print(f"V={v}, P={p} | Sigs={real_sigs} | TP={real_tp} | Score={score:.1f}")

    best = sorted(results, key=lambda x: x['score'], reverse=True)[0]
    print("\n🏆 BEST SETTING: V_Z=" + str(best['v_z']) + ", P_Z=" + str(best['p_z']) + " | Score=" + str(best['score']))
