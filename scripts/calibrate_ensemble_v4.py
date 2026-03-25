import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.ensemble import RandomForestClassifier, HistGradientBoostingClassifier
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import precision_score, recall_score
import joblib
import os

def calibrate():
    dataset_path = 'data/ml_dataset_v4.csv'
    if not os.path.exists(dataset_path):
        print("Dataset not found!")
        return
        
    df = pd.read_csv(dataset_path)
    print(f"Loaded dataset with {len(df)} samples.")
    print(f"Positive samples (True Pumps): {df['target'].sum()}")
    
    # Simple feature list (Rush Orders, Z-scores, Momentum, Whale Trade Sizes)
    features = ['std_rush_orders', 'volume_z', 'price_z', 'taker_ratio', 'volatility', 'price_momentum_3h', 'volume_momentum_3h']
    # Add Whale features if they exist in CSV
    for f in ['avg_trade_size', 'max_trade_size']:
        if f in df.columns:
            features.append(f)
            
    X = df[features]
    y = df['target']
    
    # 3-Stage Ensemble
    models = {
        'xgb': xgb.XGBClassifier(n_estimators=100, max_depth=4, learning_rate=0.05, scale_pos_weight=10),
        'rf': RandomForestClassifier(n_estimators=100, max_depth=6),
        'hgb': HistGradientBoostingClassifier(max_iter=100)
    }
    
    best_threshold = 0.85
    
    for name, model in models.items():
        print(f"Training {name}...")
        model.fit(X, y)
        joblib.dump(model, f'models/pump_{name}_v4.joblib')
        
    # Calibration Report
    print("\n--- Final Performance Report (1.5x Close Target) ---")
    preds = []
    for name, model in models.items():
        # Probability of being a pump
        p = model.predict_proba(X)[:, 1]
        preds.append(p)
        
    # Ensemble Vote (Majority 2/3)
    ensemble_prob = np.mean(preds, axis=0)
    # Actually use majority rule on binary flags for better precision
    votes = np.sum([p > 0.85 for p in preds], axis=0)
    final_signal = (votes >= 2).astype(int)
    
    tp = np.sum((final_signal == 1) & (y == 1))
    fp = np.sum((final_signal == 1) & (y == 0))
    fn = np.sum((final_signal == 0) & (y == 1))
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    
    print(f"Ensemble Precision: {precision:.2%}")
    print(f"Ensemble Recall: {recall:.2%} ({tp}/{tp+fn} pumps detected)")
    print(f"Total Signals Triggered: {np.sum(final_signal)}")
    
    if precision >= 0.90:
        print("SUCCESS: Hit 90% precision target!")
    else:
        print("WARNING: Precision below 90%. Raising hurdle threshold...")
        # Recalculate with 0.95 threshold
        votes_strict = np.sum([p > 0.95 for p in preds], axis=0)
        final_strict = (votes_strict >= 2).astype(int)
        tp_s = np.sum((final_strict == 1) & (y == 1))
        fp_s = np.sum((final_strict == 1) & (y == 0))
        precision_s = tp_s / (tp_s + fp_s) if (tp_s + fp_s) > 0 else 0
        print(f"Strict Precision (0.95): {precision_s:.2%}")

if __name__ == '__main__':
    calibrate()
