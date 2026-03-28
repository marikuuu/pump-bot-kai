import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import concurrent.futures

# ─── パラメータ ───────────────────────────────────────────────
WINDOW_MIN      = 30
HIST_CANDLES    = 100
VOL_Z_THRESH    = 3.0       # Optimized for Max Profit
PC_Z_THRESH     = 1.5       # Optimized for Max Profit
RUSH_THRESH     = 5.0
TAKER_RATIO     = 3.0
OUTCOME_DAYS    = 3
PUMP_THRESHOLD  = 1.5      # 25%以上の上昇を成功(TP)と定義

START_TS = int(datetime(2026, 3, 15, 15, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
END_TS   = int(datetime(2026, 3, 21, 15, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)

DATA_DIR = "data/history/multiverse"
BTC_FILE = os.path.join(DATA_DIR, "BTC_USDT_USDT_1m.csv")

def load_btc_crash_map():
    if not os.path.exists(BTC_FILE): return {}
    df_btc = pd.read_csv(BTC_FILE)
    df_btc = df_btc[(df_btc['timestamp'] >= START_TS - 3600000) & (df_btc['timestamp'] <= END_TS)].copy()
    df_btc['pc_15m'] = df_btc['close'].pct_change(15)
    return {row['timestamp']: row['pc_15m'] for _, row in df_btc.iterrows()}

def compute_std_rush(vol_series: pd.Series) -> float:
    if len(vol_series) < 3: return 0.0
    return float(vol_series.std())

def process_single_file(fpath, btc_crashes):
    symbol_raw = os.path.basename(fpath).replace("_USDT_USDT_1m.csv", "")
    if symbol_raw == "BTC": return []
    symbol = f"{symbol_raw}/USDT:USDT"
    
    try:
        df = pd.read_csv(fpath)
    except: return []

    df = df[(df['timestamp'] >= START_TS) & (df['timestamp'] <= END_TS)].copy()
    if len(df) < HIST_CANDLES + WINDOW_MIN: return []
    df = df.reset_index(drop=True)
    df['close'] = df['close'].astype(float)
    df['volume'] = df['volume'].astype(float)

    signals = []
    signaled_ts = None

    for i in range(HIST_CANDLES + WINDOW_MIN, len(df)):
        current_ts = int(df.iloc[i]['timestamp'])
        if btc_crashes.get(current_ts, 0) < -0.01: continue

        chunk = df.iloc[i - WINDOW_MIN: i]
        hist  = df.iloc[i - HIST_CANDLES - WINDOW_MIN: i - WINDOW_MIN]
        if len(hist) < 10: continue

        price_now = float(df.iloc[i]['close'])
        vol_hist, pc_hist = hist['volume'], hist['close'].pct_change().dropna()
        vol_chunk = chunk['volume'].mean()
        pc_chunk  = chunk['close'].pct_change().dropna().mean() if len(chunk) > 1 else 0

        vol_z = (vol_chunk - vol_hist.mean()) / (vol_hist.std() + 1e-9)
        pc_z  = (pc_chunk  - pc_hist.mean())  / (pc_hist.std()  + 1e-9)
        
        std_rush_norm = compute_std_rush(chunk['volume']) / (vol_hist.std() + 1e-9) if vol_hist.std() > 0 else 0

        # Stages
        if not (vol_z > VOL_Z_THRESH and pc_z > PC_Z_THRESH): continue
        if not (std_rush_norm > RUSH_THRESH and vol_z > 2.0): continue

        if signaled_ts and (current_ts - signaled_ts) < 60 * 60 * 1000: continue
        signaled_ts = current_ts

        # Outcome
        future = df[df['timestamp'] > current_ts]
        future_window = future[future['timestamp'] <= current_ts + OUTCOME_DAYS * 86400000]
        max_gain = (future_window['high'].astype(float).max() / price_now - 1.0) if not future_window.empty else 0.0

        signals.append({
            'symbol': symbol,
            'time': datetime.fromtimestamp(current_ts / 1000, tz=timezone.utc).strftime('%m/%d %H:%M'),
            'vol_z': round(vol_z, 2), 'pc_z': round(pc_z, 2), 'rush': round(std_rush_norm, 1),
            'max_gain%': round(max_gain * 100, 1), 'TP': max_gain >= (PUMP_THRESHOLD - 1.0)
        })
    return signals

def run_backtest():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*_USDT_USDT_1m.csv")))
    files = [f for f in files if '260327' not in f and '260626' not in f]
    
    print(f"🚀 Starting Parallel Backtest for {len(files)} symbols...")
    btc_crashes = load_btc_crash_map()
    all_signals = []

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_single_file, f, btc_crashes): f for f in files}
        for future in concurrent.futures.as_completed(futures):
            all_signals.extend(future.result())

    if not all_signals:
        print("No signals detected.")
        return

    sig_df = pd.DataFrame(all_signals).sort_values('time')
    tp = sig_df['TP'].sum()
    fp = len(sig_df) - tp
    
    print("=" * 70)
    print(f"🧪 RESULTS: Total Signals: {len(sig_df)} | TP: {tp} | FP: {fp} | Precision: {tp/len(sig_df)*100:.1f}%")
    print("=" * 70)
    print(sig_df[['symbol', 'time', 'vol_z', 'pc_z', 'max_gain%', 'TP']].to_string(index=False))
    sig_df.to_csv("blind_backtest_results.csv", index=False)

if __name__ == "__main__":
    run_backtest()
