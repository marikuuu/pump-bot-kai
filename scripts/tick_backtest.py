"""
tick_backtest.py
───────────────────────────────────────────────────────────────
tick生データを使ったリアルタイム同等バックテスト（3/16〜3/21）

★ multiverse_analyzerと同じstd_rush計算を使用:
   trades.groupby('sec').size().reindex(range(0,60), fill_value=0).std()

★ 特徴量（すべてtick生データから計算）:
   - std_rush    : 1分内の秒単位トレード数の標準偏差
   - vol_z       : 出来高のZ-score（OHLCVで計算）
   - pc_z        : 価格変化のZ-score（OHLCVで計算）
   - trade_size  : 平均取引サイズ（USDT）
   - buy_ratio   : 買いサイド取引数の割合

★ アウトカム定義:
   シグナル時刻から3日以内にシグナル価格の1.5倍以上 → TP
"""

import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timezone

# ─── パラメータ ───────────────────────────────────────────────
VOL_Z_THRESH    = 2.0
PC_Z_THRESH     = 2.0
RUSH_THRESH     = 4.0       # std_rush（秒単位の真値）。AINで15.61を観測
HIST_CANDLES    = 60        # Z-score基準の過去ローソク数
WINDOW_MIN      = 30        # シグナル検知ウィンドウ（分）
OUTCOME_DAYS    = 3         # TP確認ウィンドウ（日）
PUMP_THRESHOLD  = 1.50      # 1.5倍以上でTP

# 期間（UTC）
START_TS = int(datetime(2026, 3, 15, 15, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
END_TS   = int(datetime(2026, 3, 21, 15, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)

TICK_DIR = "data/history/multiverse/ticks"
OHLCV_DIR = "data/history/multiverse"


TICK_FILES_CACHE = {}

def preload_tick_files():
    """全tickファイルをあらかじめメモリにロードしてシンボルごとに整理する"""
    global TICK_FILES_CACHE
    print("tickファイルリストを読み込み中...")
    all_files = glob.glob(os.path.join(TICK_DIR, "*_ticks.csv"))
    for f in all_files:
        basename = os.path.basename(f)
        # Symbol extraction: everything before the timestamp
        # Format: SYMBOL_PERIOD_STAMP_ticks.csv
        parts = basename.split('_')
        if len(parts) < 4: continue
        symbol_key = "_".join(parts[:-2]) # SYMBOL_PERIOD
        if symbol_key not in TICK_FILES_CACHE:
            TICK_FILES_CACHE[symbol_key] = []
        
        try:
            ts = int(parts[-2])
            TICK_FILES_CACHE[symbol_key].append((ts, f))
        except ValueError:
            continue
    
    # Sort for faster search
    for k in TICK_FILES_CACHE:
        TICK_FILES_CACHE[k].sort()
    print(f"キャッシュ完了: {len(TICK_FILES_CACHE)} 銘柄分")

def compute_tick_features(tick_file: str) -> dict | None:
    """
    tickファイルから1分間の特徴量を計算する。
    multiverse_analyzer.pyと同じstd_rush計算。
    """
    try:
        trades = pd.read_csv(tick_file)
        if trades.empty or len(trades) < 5:
            return None

        trades['ts_dt'] = pd.to_datetime(trades['timestamp'], unit='ms')
        trades['sec'] = trades['ts_dt'].dt.second

        # std_rush: 秒ごとのトレード数の標準偏差（multiverse_analyzerと同じ）
        std_rush = trades.groupby('sec').size().reindex(range(0, 60), fill_value=0).std()

        # 取引サイズ
        avg_trade_size = float(trades['cost'].mean()) if 'cost' in trades.columns else 0.0

        # 買い比率
        buy_trades = (trades['side'] == 'buy').sum()
        buy_ratio = buy_trades / len(trades) if len(trades) > 0 else 0.5

        # 合計出来高（USDT）
        total_volume = float(trades['cost'].sum()) if 'cost' in trades.columns else 0.0

        return {
            'std_rush': float(std_rush),
            'avg_trade_size': avg_trade_size,
            'buy_ratio': float(buy_ratio),
            'total_volume': total_volume,
            'n_trades': len(trades),
        }
    except Exception as e:
        return None


def find_tick_file(symbol_clean: str, ts_ms: int) -> str | None:
    """
    キャッシュから指定タイムスタンプ近辺のtickファイルを探す（±30分以内）
    OHLCVのStage2判定時刻とtickファイルは平均20分ずれるため。
    """
    if symbol_clean not in TICK_FILES_CACHE:
        return None

    files = TICK_FILES_CACHE[symbol_clean]
    
    best_file = None
    best_diff = float('inf')

    for file_ts, fpath in files:
        diff = abs(file_ts - ts_ms)
        if diff < best_diff:
            best_diff = diff
            best_file = fpath
        
        # Sorted list, skip once we're 30min past the target
        if file_ts > ts_ms + 30 * 60 * 1000:
            break

    if best_diff < 30 * 60 * 1000:  # 30分以内
        return best_file
    return None


def run_backtest():
    preload_tick_files()
    files = sorted(glob.glob(os.path.join(OHLCV_DIR, "*_USDT_USDT_1m.csv")))
    # 期限付き先物除外
    files = [f for f in files if '260327' not in f and '260626' not in f]

    print(f"対象銘柄数 (全): {len(files)}")
    # tickデータがある銘柄のみに絞る
    files = [f for f in files if os.path.basename(f).replace("_1m.csv", "") in TICK_FILES_CACHE]
    print(f"対象銘柄数 (tickあり): {len(files)}")
    print("バックテスト実行中（主要銘柄限定・即時出力）...")

    all_signals = []

    for fpath in files:
        symbol_raw = os.path.basename(fpath).replace("_USDT_USDT_1m.csv", "")
        symbol = f"{symbol_raw}/USDT:USDT"
        symbol_clean = f"{symbol_raw}_USDT_USDT"
        
        # print(f"Scanning {symbol}...")

        try:
            df = pd.read_csv(fpath)
        except Exception:
            continue

        df = df[(df['timestamp'] >= START_TS) & (df['timestamp'] <= END_TS)].copy()
        if len(df) < HIST_CANDLES + WINDOW_MIN:
            continue

        df = df.reset_index(drop=True)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        df['high'] = df['high'].astype(float)

        signaled_ts = None  # クールダウン用

        for i in range(HIST_CANDLES + WINDOW_MIN, len(df)):
            chunk = df.iloc[i - WINDOW_MIN: i]
            hist  = df.iloc[i - HIST_CANDLES - WINDOW_MIN: i - WINDOW_MIN]

            if len(hist) < 10:
                continue

            current_ts = int(df.iloc[i]['timestamp'])
            price_now  = float(df.iloc[i]['close'])

            # Z-score（OHLCVから）
            vol_hist = hist['volume']
            pc_hist  = hist['close'].pct_change().dropna()

            vol_chunk = chunk['volume'].mean()
            pc_chunk  = chunk['close'].pct_change().dropna().mean() if len(chunk) > 1 else 0

            vol_mean, vol_std = vol_hist.mean(), vol_hist.std()
            pc_mean, pc_std   = pc_hist.mean(), pc_hist.std()

            vol_z = (vol_chunk - vol_mean) / (vol_std + 1e-9)
            pc_z  = (pc_chunk - pc_mean)  / (pc_std + 1e-9)

            # Stage 2: Z-score フィルター
            s2 = (vol_z > VOL_Z_THRESH and pc_z > PC_Z_THRESH)
            if not s2:
                continue

            # DEBUG: Stage 2 Passed
            # print(f"DEBUG: Stage 2 passed for {symbol} at {current_ts}")

            # Stage 3: tickデータから真のstd_rushを計算
            tick_file = find_tick_file(symbol_clean, current_ts)
            if tick_file is None:
                # tickファイルがなければOHLCV近似でフォールバック
                tick_feats = None
                std_rush = 0.0
                buy_ratio = 0.5
                avg_trade_size = 0.0
            else:
                tick_feats = compute_tick_features(tick_file)
                if tick_feats is None:
                    std_rush = 0.0
                    buy_ratio = 0.5
                    avg_trade_size = 0.0
                else:
                    std_rush = tick_feats['std_rush']
                    buy_ratio = tick_feats['buy_ratio']
                    avg_trade_size = tick_feats['avg_trade_size']

            # Stage 3 判定: std_rush のみで判定
            # 先物市場ではbuy/sell比が50:50になりやすいため buy_ratio は使わない
            s3 = std_rush > RUSH_THRESH

            if not s3:
                continue

            # クールダウン（同一銘柄 60分以内）
            if signaled_ts and (current_ts - signaled_ts) < 60 * 60 * 1000:
                continue
            signaled_ts = current_ts

            # アウトカム計算
            future_end_ts = current_ts + OUTCOME_DAYS * 24 * 60 * 60 * 1000
            future = df[df['timestamp'] > current_ts]
            future_window = future[future['timestamp'] <= future_end_ts]

            if len(future_window) > 0:
                max_future_price = future_window['high'].max()
                max_gain = (max_future_price / price_now) - 1.0
                is_pump  = max_future_price >= price_now * PUMP_THRESHOLD
            else:
                max_gain = 0.0
                is_pump  = False

            signal_dt = datetime.fromtimestamp(current_ts / 1000, tz=timezone.utc).strftime('%m/%d %H:%M UTC')
            sig_obj = {
                'symbol':         symbol,
                'signal_time':    signal_dt,
                'price':          price_now,
                'vol_z':          round(vol_z, 2),
                'pc_z':           round(pc_z, 2),
                'std_rush':       round(std_rush, 1),
                'buy_ratio':      round(buy_ratio, 3),
                'avg_trade_size': round(avg_trade_size, 2),
                'max_gain%':      round(max_gain * 100, 1),
                'TP':             is_pump,
                'has_tick':       tick_file is not None,
            }
            all_signals.append(sig_obj)
            
            # 即時出力
            tp_mark = "✅" if is_pump else "❌"
            print(f"  [SIGNAL] {symbol:<15} {signal_dt} ${price_now:>8.5f} Rush={std_rush:>5.1f} Gain={max_gain*100:>6.1f}% {tp_mark}")

    # ── 集計 ──
    if not all_signals:
        print("⚠️  シグナルが1件も検出されませんでした。")
        print("   → 閾値が高すぎる可能性があります。RUSH_THRESH を下げてみてください。")
        return

    sig_df = pd.DataFrame(all_signals)
    tp = sig_df['TP'].sum()
    fp = len(sig_df) - tp
    precision = tp / len(sig_df) * 100
    tick_coverage = sig_df['has_tick'].sum() / len(sig_df) * 100

    print("=" * 70)
    print("🧪 TICK BACKTEST RESULTS (3/16〜3/21) - tick生データ使用")
    print("=" * 70)
    print(f"  期間          : 2026-03-16 00:00 〜 2026-03-21 23:59 JST")
    print(f"  対象銘柄数    : {len(files)}")
    print(f"  総シグナル    : {len(sig_df)}")
    print(f"  TP (1.5x達成) : {int(tp)}")
    print(f"  FP (未達)     : {int(fp)}")
    print(f"  Precision     : {precision:.1f}%")
    print(f"  tick補足率    : {tick_coverage:.1f}% (tickファイルがあったシグナル)")
    print()
    print("─── シグナル一覧 ─────────────────────────────────────────────")
    header = f"{'Symbol':<25} {'Time':<16} {'Price':>8} {'VolZ':>6} {'PcZ':>6} {'Rush':>6} {'Buy%':>6} {'MaxGain%':>9} TP"
    print(header)
    print("-" * 95)

    for _, r in sig_df.sort_values('signal_time').iterrows():
        tp_mark = "✅" if r['TP'] else "❌"
        tick_mark = "●" if r['has_tick'] else "○"
        print(f"{r['symbol']:<25} {r['signal_time']:<16} ${r['price']:>8.5f} "
              f"{r['vol_z']:>6.2f} {r['pc_z']:>6.2f} {r['std_rush']:>6.1f} "
              f"{r['buy_ratio']*100:>5.1f}% {r['max_gain%']:>8.1f}% {tp_mark}{tick_mark}")

    print()
    if tp > 0:
        print("─── TP (1.5x達成) の内訳 ───")
        for _, r in sig_df[sig_df['TP']].sort_values('max_gain%', ascending=False).iterrows():
            print(f"  🎯 {r['symbol']:<25} at {r['signal_time']} → +{r['max_gain%']:.1f}%  rush={r['std_rush']}")

    out_path = "tick_backtest_results.csv"
    sig_df.to_csv(out_path, index=False)
    print(f"\n📄 詳細結果: {out_path}")
    print("   ● = tickファイルあり, ○ = OHLCVフォールバック")


if __name__ == "__main__":
    run_backtest()
