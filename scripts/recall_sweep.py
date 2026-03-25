import pandas as pd
import pickle
import numpy as np

def perform_recall_sweep():
    # Load v5.1 training data
    df = pd.read_csv('tick_training_data_v51.csv')
    with open('pump_ai/pump_model_v51_diverse.pkl', 'rb') as f:
        model = pickle.load(f)

    FEATURES = ['vol_z', 'pc_z', 'pre_accum_z', 'std_rush', 'avg_trade_size', 'max_trade_size', 'median_trade_size', 'buy_ratio', 'acceleration', 'price_impact']
    X = df[FEATURES].fillna(0)
    y = df['label']
    
    probs = model.predict_proba(X)[:, 1]
    
    print("--- v5.1 Precision/Recall Sweep Report ---")
    print(f"{'Thresh':<10} | {'Signals':<10} | {'Recall':<10} | {'Precision':<10}")
    print("-" * 50)
    
    total_positives = len(df[df['label']==1]) # occurrences
    
    for thresh in [0.95, 0.85, 0.70, 0.50, 0.30, 0.197]:
        hits = probs >= thresh
        tp = np.sum((hits == 1) & (y == 1))
        fp = np.sum((hits == 1) & (y == 0))
        
        prec = tp / (tp + fp) if (tp + fp) > 0 else 1.0
        rec = tp / total_positives if total_positives > 0 else 0
        
        print(f"{thresh:<10.3f} | {np.sum(hits):<10} | {rec:<10.2%} | {prec:<10.2%}")

if __name__ == "__main__":
    perform_recall_sweep()
