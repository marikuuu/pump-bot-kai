import pandas as pd
import json
import os
from datetime import datetime, timedelta

def list_training_hits():
    # Load multiverse candidates to map timestamps to symbols
    with open('multiverse_candidates.json', 'r') as f:
        candidates = json.load(f)
    
    # Load the ML dataset
    df = pd.read_csv('data/ml_dataset_v4.csv').sort_values('timestamp')
    
    # Split into 80/20 to find training set
    split_idx = int(len(df) * 0.8)
    train_df = df.iloc[:split_idx]
    
    # Since we found 100% precision on the training set in the last backtest
    # at a specific threshold, we need that threshold or just all target=1 if precision was 100%
    # But wait! 'target' is the actual result. The AI 'hits' are based on the model.
    
    # Let's just find the cases where target=1 in the training set
    # OR better yet, run the classifier again to find the actual signals.
    import joblib
    models_dir = 'models/'
    xgb_model = joblib.load(os.path.join(models_dir, 'pump_xgb.joblib'))
    rf_model = joblib.load(os.path.join(models_dir, 'pump_rf.joblib'))
    hgb_model = joblib.load(os.path.join(models_dir, 'pump_hgb.joblib'))
    
    threshold = 0.197 # From the 'Best Training Precision' sweep result
    
    features = ['std_rush_orders', 'taker_ratio', 'avg_trade_size', 'max_trade_size', 'volume_z', 'price_z', 'momentum']
    X_train = train_df[features]
    
    xgb_p = xgb_model.predict_proba(X_train)[:, 1] >= threshold
    rf_p = rf_model.predict_proba(X_train)[:, 1] >= threshold
    hgb_p = hgb_model.predict_proba(X_train)[:, 1] >= threshold
    votes = xgb_p.astype(int) + rf_p.astype(int) + hgb_p.astype(int)
    signals = (votes >= 2)
    
    hits = []
    for i in range(len(train_df)):
        if signals[i]:
            ts = train_df.iloc[i]['timestamp']
            is_tp = (train_df.iloc[i]['target'] == 1)
            jst = pd.to_datetime(ts, unit='ms') + timedelta(hours=9)
            
            # Match symbol
            match = [c['symbol'] for c in candidates if abs(c['start_ts'] - ts) < 1000]
            sym = match[0] if match else "Unknown"
            
            hits.append({
                'symbol': sym,
                'jst': jst.strftime('%Y-%m-%d %H:%M:%S'),
                'type': "HIT (TP)" if is_tp else "MISS (FP)"
            })
            
    # Output to file
    with open('training_hits_list.json', 'w') as f:
        json.dump(hits, f, indent=4)
    
    print(f"Extracted {len(hits)} signals from training set.")

import os
if __name__ == '__main__':
    list_training_hits()
