import os
import glob
import pandas as pd
import numpy as np
import pickle
from datetime import datetime, timezone
import concurrent.futures
import sys

# --- モデル読み込みの準備 ---
MODEL_PATH = 'pump_ai/pump_model_v51_diverse.pkl'
if not os.path.exists(MODEL_PATH):
    print(f"Error: Model not found at {MODEL_PATH}")
    sys.exit()

with open(MODEL_PATH, 'rb') as f:
    clf = pickle.load(f)

# ─── パラメータ ───────────────────────────────────────────────
HIST_CANDLES    = 100
WINDOW_MIN      = 30
VOL_Z_THRESH    = 3.0
PC_Z_THRESH     = 1.5
ML_THRESHOLD    = 0.85      # detector.py と同じしきい値
PUMP_THRESHOLD  = 1.25

START_TS = int(datetime(2026, 3, 15, 15, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
END_TS   = int(datetime(2026, 3, 21, 15, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
DATA_DIR = "data/history/multiverse"

def compute_features(df, i):
    # 現時点の価格
    price_now = float(df.iloc[i]['close'])
    
    # ヒストリカルデータ
    hist = df.iloc[i - HIST_CANDLES - WINDOW_MIN : i - WINDOW_MIN]
    chunk = df.iloc[i - WINDOW_MIN : i]
    
    vol_hist, pc_hist = hist['volume'], hist['close'].pct_change().dropna()
    vol_chunk = chunk['volume'].mean()
    pc_chunk = chunk['close'].pct_change().dropna().mean() if len(chunk) > 1 else 0
    
    # Z-scores
    vol_z = (vol_chunk - vol_hist.mean()) / (vol_hist.std() + 1e-9)
    pc_z = (pc_chunk - pc_hist.mean()) / (pc_hist.std() + 1e-9)
    
    # Pre-accumulation Z-score (後半の出来高 / 前半の出来高)
    half = len(hist) // 2
    pre_vol_mid = hist['volume'].iloc[:half].mean()
    pre_vol_late = hist['volume'].iloc[half:].mean()
    pre_accum_z = (pre_vol_late - pre_vol_mid) / (hist['volume'].std() + 1e-9)
    
    # Price Impact (High vs Open ratio)
    price_impact = (df.iloc[i]['high'] / df.iloc[i]['open'] - 1.0)
    
    # Tick-level features (1m足からは推測値を使用)
    std_rush = (chunk['volume'].std() / (vol_hist.std() + 1e-9)) * 5.0 # スケーリング
    
    # 推論用ベクトル (detector.py と同じ順番)
    X = np.array([[
        vol_z, pc_z, pre_accum_z,
        std_rush,           # std_rush (proxy)
        vol_chunk / 10,     # avg_trade_size (dummy/proxy)
        vol_chunk,          # max_trade_size (dummy/proxy)
        vol_chunk / 20,     # median_trade_size (dummy/proxy)
        0.75,               # buy_ratio (assume 0.75 for candidate)
        1.5,                # acceleration (assume 1.5)
        price_impact
    ]])
    
    return X, vol_z, pc_z, price_now

def process_single_file(fpath):
    symbol_raw = os.path.basename(fpath).replace("_USDT_USDT_1m.csv", "")
    if symbol_raw == "BTC": return []
    symbol = f"{symbol_raw}/USDT:USDT"
    
    try:
        df = pd.read_csv(fpath)
    except: return []

    df = df[(df['timestamp'] >= START_TS) & (df['timestamp'] <= END_TS)].copy()
    if len(df) < HIST_CANDLES + WINDOW_MIN: return []
    df = df.reset_index(drop=True)

    signals = []
    signaled_ts = None

    for i in range(HIST_CANDLES + WINDOW_MIN, len(df)):
        current_ts = int(df.iloc[i]['timestamp'])
        
        # 特徴量計算
        X, vol_z, pc_z, price_now = compute_features(df, i)
        
        # Stage 2: 統計フィルタ
        if not (vol_z > VOL_Z_THRESH and pc_z > PC_Z_THRESH): continue
        
        # Stage 3: ML フィルタ
        try:
            proba = clf.predict_proba(X)[0, 1]
            if proba < ML_THRESHOLD: continue
        except:
            continue

        if signaled_ts and (current_ts - signaled_ts) < 60 * 60 * 1000: continue
        signaled_ts = current_ts

        # Outcome (3日後の上昇)
        future = df[df['timestamp'] > current_ts]
        future_window = future[future['timestamp'] <= current_ts + 3 * 86400000]
        max_gain = (future_window['high'].max() / price_now - 1.0) if not future_window.empty else 0.0

        signals.append({
            'symbol': symbol,
            'time': datetime.fromtimestamp(current_ts / 1000, tz=timezone.utc).strftime('%m/%d %H:%M'),
            'vol_z': round(vol_z, 2), 'pc_z': round(pc_z, 2), 'ml_proba': round(proba, 4),
            'max_gain%': round(max_gain * 100, 1), 'TP': max_gain >= (PUMP_THRESHOLD - 1.0)
        })
    return signals

def run_backtest_ai():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*_USDT_USDT_1m.csv")))
    files = [f for f in files if '260327' not in f and '260626' not in f]
    
    print(f"🧠 Starting Backtest with AI (Stage 3) | {len(files)} symbols...")
    
    all_signals = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_single_file, f): f for f in files}
        for future in concurrent.futures.as_completed(futures):
            all_signals.extend(future.result())

    if not all_signals:
        print("No AI signals detected.")
        return

    sig_df = pd.DataFrame(all_signals).sort_values('time')
    tp = sig_df['TP'].sum()
    fp = len(sig_df) - tp
    precision = (tp / len(sig_df)) * 100
    
    print("=" * 70)
    print(f"🎯 AI BACKTEST RESULTS: Total Signals: {len(sig_df)} | TP: {tp} | FP: {fp} | Precision: {precision:.1f}%")
    print("=" * 70)
    print(sig_df[['symbol', 'time', 'ml_proba', 'max_gain%', 'TP']].to_string(index=False))
    
    sig_df.to_csv("ai_backtest_results.csv", index=False)

if __name__ == "__main__":
    run_backtest_ai()
