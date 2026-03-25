import pandas as pd
import numpy as np
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import precision_score, recall_score
import sys
import os

# Ensure local imports work
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pump_ai.pump_classifier import PumpClassifier

def run_backtest():
    dataset_path = 'data/ml_dataset_v4.csv'
    if not os.path.exists(dataset_path):
        print("Dataset not found! Run training first.")
        return
        
    df = pd.read_csv(dataset_path).sort_values('timestamp')
    print(f"Loaded {len(df)} samples from {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    features = ['std_rush_orders', 'taker_ratio', 'avg_trade_size', 'max_trade_size', 'volume_z', 'price_z', 'momentum']
    
    # 80/20 Chronological Split
    split_idx = int(len(df) * 0.8)
    train_df = df.iloc[:split_idx]
    test_df = df.iloc[split_idx:]
    
    print(f"Train samples: {len(train_df)} (Pos: {train_df['target'].sum()})")
    print(f"Test samples: {len(test_df)} (Pos: {test_df['target'].sum()})")
    
    X_train, y_train = train_df[features], train_df['target']
    X_test, y_test = test_df[features], test_df['target']
    
    # Initialize and Train
    classifier = PumpClassifier()
    classifier.train(X_train, y_train)
    
    # Sweep Thresholds for Max Precision
    print("\n--- Sweeping Thresholds for Max Precision (Adjusted for Calibrated Probs) ---")
    best_p = 0
    best_t = 0.5
    for t in np.linspace(0.05, 0.5, 50):
        # Apply same threshold to all 3 models for simplicity in sweep
        xgb_p = classifier.xgb_model.predict_proba(X_train)[:, 1] >= t
        rf_p = classifier.rf_model.predict_proba(X_train)[:, 1] >= t
        hgb_p = classifier.hgb_model.predict_proba(X_train)[:, 1] >= t
        votes = xgb_p.astype(int) + rf_p.astype(int) + hgb_p.astype(int)
        preds = (votes >= 2).astype(int)
        
        tp = np.sum((preds == 1) & (y_train == 1))
        fp = np.sum((preds == 1) & (y_train == 0))
        if (tp + fp) >= 10: # Higher stability
            p = tp / (tp + fp)
            if p > best_p:
                best_p = p
                best_t = t
                
    print(f"Best Training Precision: {best_p:.1%} at Threshold: {best_t:.3f}")
    
    # Evaluate on TEST SET
    print("\n--- HOLD-OUT TEST SET RESULTS (UNSEEN DATA) ---")
    xgb_p = classifier.xgb_model.predict_proba(X_test)[:, 1] >= best_t
    rf_p = classifier.rf_model.predict_proba(X_test)[:, 1] >= best_t
    hgb_p = classifier.hgb_model.predict_proba(X_test)[:, 1] >= best_t
    
    test_signals = ((xgb_p.astype(int) + rf_p.astype(int) + hgb_p.astype(int)) >= 2).astype(int)
    
    print(f"Signals Triggered: {np.sum(test_signals)}")
    if np.sum(test_signals) > 0:
        import json
        with open('multiverse_candidates.json', 'r') as f:
            candidates = json.load(f)
        for i in np.where(test_signals == 1)[0]:
            is_tp = (y_test.iloc[i] == 1)
            ts = test_df.iloc[i]['timestamp']
            match = [c['symbol'] for c in candidates if abs(c['start_ts'] - ts) < 1000]
            label = "HIT (TP)" if is_tp else "MISS (FP)"
            print(f"{label}: {match[0] if match else 'Unknown':<20} | Time: {pd.to_datetime(ts, unit='ms')+pd.Timedelta(hours=9)}")
    
    if tp > 0:
        print("\n--- Successful Test Set Detections (HITS) ---")
        import json
        with open('multiverse_candidates.json', 'r') as f:
            candidates = json.load(f)
        for i in np.where((test_signals == 1) & (y_test == 1))[0]:
            ts = test_df.iloc[i]['timestamp']
            match = [c['symbol'] for c in candidates if abs(c['start_ts'] - ts) < 1000]
            print(f"HIT: {match[0] if match else 'Unknown':<20} | Time: {pd.to_datetime(ts, unit='ms')+pd.Timedelta(hours=9)}")

if __name__ == '__main__':
    run_backtest()
