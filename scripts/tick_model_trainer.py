"""
tick_model_trainer.py
───────────────────────────────────────────────────────────────
tick生データから真の特徴量を抽出し、XGBoostモデルを再訓練する。

★ 使用特徴量（すべてtick生データから計算）:
   - std_rush      : 秒単位buy-tradeカウントの標準偏差（真のRush計算）
   - avg_trade_size: 平均取引サイズ（USDT）
   - max_trade_size: 最大取引サイズ（大口クジラの痕跡）
   - buy_ratio     : 買いサイド取引数の割合（テイカー圧力）
   - vol_z         : 出来高Z-score（OHLCV）
   - pc_z          : 価格変化Z-score（OHLCV）
   - pre_accum_z   : 3時間前からの出来高累積変化率

★ ラベル:
   シグナルからOUTCOME_DAYS日以内に1.5倍以上 → 1 (pump)
   それ以外 → 0

★ 実行:
   python scripts/tick_model_trainer.py
"""

import os
import glob
import json
import pickle
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

TICK_DIR  = "data/history/multiverse/ticks"
OHLCV_DIR = "data/history/multiverse"

# 学習データ設定
TRAIN_START_TS = int(datetime(2026, 3, 14, 15, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
TRAIN_END_TS   = int(datetime(2026, 3, 21, 15, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
OUTCOME_DAYS   = 3
PUMP_THRESHOLD = 1.50

# Stage 2 フィルター（データ収集時に使う）
VOL_Z_THRESH   = 1.5   # 訓練時はやや緩めに設定して多くのサンプルを収集
PC_Z_THRESH    = 1.5

HIST_CANDLES   = 60
WINDOW_MIN     = 30

MODEL_OUTPUT = "pump_ai/pump_model_v5_tick.pkl"
FEATURES_OUTPUT = "pump_ai/pump_model_v5_tick_features.json"


TICK_FILES_CACHE = {}

def preload_tick_files():
    """全tickファイルをあらかじめメモリにロードしてシンボルごとに整理する"""
    global TICK_FILES_CACHE
    logger.info("tickファイルリストを読み込み中...")
    all_files = glob.glob(os.path.join(TICK_DIR, "*_ticks.csv"))
    for f in all_files:
        basename = os.path.basename(f)
        parts = basename.split('_')
        if len(parts) < 4: continue
        symbol_key = "_".join(parts[:-2])
        if symbol_key not in TICK_FILES_CACHE:
            TICK_FILES_CACHE[symbol_key] = []
        try:
            ts = int(parts[-2])
            TICK_FILES_CACHE[symbol_key].append((ts, f))
        except ValueError:
            continue
    for k in TICK_FILES_CACHE:
        TICK_FILES_CACHE[k].sort()
    logger.info(f"キャッシュ完了: {len(TICK_FILES_CACHE)} 銘柄分")


def find_tick_file_exact(symbol_clean: str, ts_ms: int) -> str | None:
    """キャッシュからタイムスタンプと最も近いtickファイルを探す（±30分）"""
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
        if file_ts > ts_ms + 30 * 60 * 1000:
            break

    if best_diff < 30 * 60 * 1000:
        return best_file
    return None


from functools import lru_cache

@lru_cache(maxsize=1000)
def compute_tick_features_v2(tick_file: str) -> dict | None:
    """tickファイルから詳細特徴量を計算 (LRUキャッシュで高速化)"""
    try:
        trades = pd.read_csv(tick_file)
        if trades.empty or len(trades) < 5:
            return None

        trades['ts_dt'] = pd.to_datetime(trades['timestamp'], unit='ms')
        trades['sec'] = trades['ts_dt'].dt.second
        
        buy_per_sec = trades.groupby('sec').size().reindex(range(0, 60), fill_value=0)
        std_rush = float(buy_per_sec.std())

        costs = pd.to_numeric(trades['cost'], errors='coerce').fillna(0)
        avg_trade_size = float(costs.mean()) if not costs.empty else 0.0
        max_trade_size = float(costs.max()) if not costs.empty else 0.0
        median_trade_size = float(costs.median()) if not costs.empty else 0.0

        buy_count = (trades['side'] == 'buy').sum()
        buy_ratio = buy_count / (len(trades) + 1e-9)
        
        acceleration = (trades[trades['sec'] >= 30].shape[0] + 1) / (trades[trades['sec'] < 30].shape[0] + 1)

        prices = pd.to_numeric(trades['price'], errors='coerce').dropna()
        price_impact = (prices.max() - prices.iloc[0]) / (prices.iloc[0] + 1e-9) if len(prices) >= 2 else 0.0

        return (std_rush, avg_trade_size, max_trade_size, median_trade_size, float(buy_ratio), float(acceleration), float(price_impact), len(trades))
    except Exception as e:
        return None


def collect_training_data() -> pd.DataFrame:
    """
    全銘柄のOHLCVデータをスキャンして、Stage 2を通過したイベントの
    tick特徴量とラベルを収集する。
    """
    preload_tick_files()
    files = sorted(glob.glob(os.path.join(OHLCV_DIR, "*_USDT_USDT_1m.csv")))
    files = [f for f in files if '260327' not in f and '260626' not in f]
    
    # tickデータがある銘柄のみに絞る（高速化）
    files = [f for f in files if os.path.basename(f).replace("_1m.csv", "") in TICK_FILES_CACHE]

    logger.info(f"対象銘柄 (tickあり): {len(files)}件")

    all_rows = []
    tick_hit, tick_miss = 0, 0

    for idx, fpath in enumerate(files):
        symbol_raw = os.path.basename(fpath).replace("_USDT_USDT_1m.csv", "")
        symbol_clean = f"{symbol_raw}_USDT_USDT"
        symbol = f"{symbol_raw}/USDT:USDT"

        if idx % 10 == 0:
            logger.info(f"Processing {idx}/{len(files)}: {symbol}...")

        try:
            df = pd.read_csv(fpath)
        except Exception:
            continue

        df = df[(df['timestamp'] >= TRAIN_START_TS) & (df['timestamp'] <= TRAIN_END_TS)].copy()
        if len(df) < HIST_CANDLES + WINDOW_MIN:
            continue

        df = df.reset_index(drop=True)
        df['close']  = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        df['high']   = df['high'].astype(float)

        for i in range(HIST_CANDLES + WINDOW_MIN, len(df)):
            chunk = df.iloc[i - WINDOW_MIN: i]
            hist  = df.iloc[i - HIST_CANDLES - WINDOW_MIN: i - WINDOW_MIN]

            if len(hist) < 10:
                continue

            current_ts = int(df.iloc[i]['timestamp'])
            price_now  = float(df.iloc[i]['close'])

            # Z-score（OHLCV）
            vol_hist = hist['volume']
            pc_hist  = hist['close'].pct_change().dropna()

            vol_chunk = chunk['volume'].mean()
            pc_chunk  = chunk['close'].pct_change().dropna().mean() if len(chunk) > 1 else 0

            vol_mean, vol_std = vol_hist.mean(), vol_hist.std()
            pc_mean,  pc_std  = pc_hist.mean(),  pc_hist.std()

            vol_z = (vol_chunk - vol_mean) / (vol_std + 1e-9)
            pc_z  = (pc_chunk - pc_mean)  / (pc_std  + 1e-9)

            # Stage 2 フィルター（やや緩め）
            s2 = (vol_z > VOL_Z_THRESH and pc_z > PC_Z_THRESH)
            if not s2:
                continue

            # Pre-accumulation: 3時間前からの出来高増加率
            pre_start = max(0, i - HIST_CANDLES - WINDOW_MIN - 180)
            pre_end   = i - HIST_CANDLES - WINDOW_MIN
            pre_window = df.iloc[pre_start:pre_end]
            if len(pre_window) >= 30:
                pre_vol_mid = pre_window['volume'].iloc[:len(pre_window)//2].mean()
                pre_vol_late = pre_window['volume'].iloc[len(pre_window)//2:].mean()
                pre_accum_z = (pre_vol_late - pre_vol_mid) / (pre_window['volume'].std() + 1e-9)
            else:
                pre_accum_z = 0.0

            # tick特徴量
            tick_file = find_tick_file_exact(symbol_clean, current_ts)
            res = None
            if tick_file:
                res = compute_tick_features_v2(tick_file)
            
            if res:
                std_rush, avg_trade_size, max_trade_size, median_trade_size, buy_ratio, acceleration, price_impact, n_trades = res
                tick_hit += 1
            else:
                std_rush, avg_trade_size, max_trade_size, median_trade_size, buy_ratio, acceleration, price_impact, n_trades = 0, 0, 0, 0, 0.5, 1.0, 0.0, 0
                tick_miss += 1

            # アウトカムラベル
            future_end_ts = current_ts + OUTCOME_DAYS * 24 * 60 * 60 * 1000
            future = df[(df['timestamp'] > current_ts) & (df['timestamp'] <= future_end_ts)]

            if len(future) > 0:
                max_future_price = future['high'].max()
                is_pump = 1 if max_future_price >= price_now * PUMP_THRESHOLD else 0
            else:
                is_pump = 0

            row = {
                'symbol': symbol,
                'timestamp': current_ts,
                'price': price_now,
                'vol_z': vol_z,
                'pc_z': pc_z,
                'pre_accum_z': pre_accum_z,
                'std_rush': std_rush,
                'avg_trade_size': avg_trade_size,
                'max_trade_size': max_trade_size,
                'median_trade_size': median_trade_size,
                'buy_ratio': buy_ratio,
                'acceleration': acceleration,
                'price_impact': price_impact,
                'label': is_pump,
            }
            all_rows.append(row)

    train_df = pd.DataFrame(all_rows)
    logger.info(f"収集サンプル数: {len(train_df)} (tick補足: {tick_hit}, 未補足: {tick_miss})")
    if not train_df.empty:
        pump_ratio = train_df['label'].mean()
        logger.info(f"ポンプ率: {pump_ratio:.3%} ({train_df['label'].sum():.0f} / {len(train_df)})")
    return train_df


def train_model(train_df: pd.DataFrame):
    """XGBoostでモデルを訓練して保存する"""
    try:
        import xgboost as xgb
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import precision_score, recall_score, average_precision_score
    except ImportError:
        logger.error("xgboost / scikit-learn が必要です: pip install xgboost scikit-learn")
        return

    FEATURES = [
        'vol_z', 'pc_z', 'pre_accum_z',
        'std_rush', 'avg_trade_size', 'max_trade_size', 'median_trade_size',
        'buy_ratio', 'acceleration', 'price_impact',
    ]

    # シャッフルして偏りをなくす
    train_df = train_df.sample(frac=1, random_state=42).reset_index(drop=True)

    X = train_df[FEATURES].fillna(0)
    y = train_df['label']

    if y.sum() == 0:
        logger.error("ポンプサンプルが0件です。閾値を下げてください。")
        return

    # 分割（シャッフル済みなので単純分割でOK）
    split_idx = int(len(train_df) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    scale_pos_weight = (len(y_train) - y_train.sum()) / (y_train.sum() + 1e-9)
    logger.info(f"scale_pos_weight: {scale_pos_weight:.1f}")

    model = xgb.XGBClassifier(
        objective='binary:logistic',
        eval_metric='aucpr',
        max_depth=5,
        learning_rate=0.05,
        n_estimators=200,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos_weight,
        use_label_encoder=False,
        verbosity=0,
    )

    logger.info("モデル訓練中...")
    model.fit(X_train, y_train,
              eval_set=[(X_test, y_test)],
              verbose=False)

    # 評価
    probas = model.predict_proba(X_test)[:, 1]
    for thresh in [0.5, 0.7, 0.85, 0.9]:
        preds = (probas >= thresh).astype(int)
        if preds.sum() > 0:
            prec = precision_score(y_test, preds, zero_division=0)
            rec  = recall_score(y_test, preds, zero_division=0)
            logger.info(f"threshold={thresh:.2f}  Precision={prec:.3f}  Recall={rec:.3f}  Signals={preds.sum()}")

    pr_auc = average_precision_score(y_test, probas)
    logger.info(f"PR-AUC: {pr_auc:.4f}")

    # 保存
    os.makedirs("pump_ai", exist_ok=True)
    with open(MODEL_OUTPUT, 'wb') as f:
        pickle.dump(model, f)
    with open(FEATURES_OUTPUT, 'w') as f:
        json.dump(FEATURES, f)

    logger.info(f"✅ モデル保存: {MODEL_OUTPUT}")
    logger.info(f"✅ 特徴量リスト保存: {FEATURES_OUTPUT}")

    # 特徴量重要度表示
    importance = pd.Series(model.feature_importances_, index=FEATURES).sort_values(ascending=False)
    print("\n--- 特徴量重要度 ---")
    for feat, imp in importance.items():
        bar = "█" * int(imp * 50)
        print(f"  {feat:<22} {imp:.4f} {bar}")


if __name__ == "__main__":
    logger.info("=== tick_model_trainer.py 開始 ===")

    # 1. 訓練データ収集
    train_df = collect_training_data()

    if train_df.empty:
        logger.error("データが収集できませんでした。")
        exit(1)

    # データ保存（デバッグ用）
    train_df.to_csv("tick_training_data.csv", index=False)
    logger.info("訓練データ保存: tick_training_data.csv")

    # 2. モデル訓練
    train_model(train_df)
