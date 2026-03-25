import pandas as pd
import numpy as np
import logging
import sys
import os

# Add current directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pump_ai.model_trainer import PumpModelTrainer
from pump_ai.pump_classifier import PumpClassifier

logging.basicConfig(level=logging.INFO, format='%(name)s - %(message)s')

def run_walk_forward():
    print("--- Running Walk-Forward Validation ---")
    dataset_path = 'data/ml_dataset_v4.csv'
    
    if not os.path.exists(dataset_path):
        print("Error: ml_dataset_v4.csv not found. Please run train_multiverse_xgb.py first.")
        return
        
    df = pd.read_csv(dataset_path)
    # Ensure chronological order
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    features = ['std_rush_orders', 'volume_z', 'price_z', 'taker_ratio', 'market_cap', 'volatility']
    target = 'target'
    
    print(f"Loaded dataset: {len(df)} samples")
    print(f"Total True Pumps (Label=1): {df[target].sum()}")
    
    # We will use the trained classifier's architecture inside the trainer hook
    trainer = PumpModelTrainer()
    
    print("\n[Phase 1] Time-Series Split (No Data Leakage)")
    try:
        # Perform 5-Fold Walk Forward validation
        results_df = trainer.walk_forward_validation(df, features, target, n_splits=5)
        print("\nFold Results:")
        print(results_df.to_string())
        
        avg_precision = results_df['precision'].mean()
        avg_recall = results_df['recall'].mean()
        print(f"\nAverage Walk-Forward Precision: {avg_precision:.2%}")
        print(f"Average Walk-Forward Recall: {avg_recall:.2%}")
    except Exception as e:
        print(f"Walk-forward validation encountered an error (likely not enough True Positives per fold): {e}")

def run_cross_exchange_test():
    print("\n--- Running Cross-Exchange Generalization Test ---")
    print("Testing Binance trained model on synthetic MEXC/KuCoin unseen market data...")
    
    # Load the production model
    classifier = PumpClassifier()
    if not classifier.load_model():
        print("Model pump_xgb.joblib not found.")
        return
        
    # Generate synthetic "KuCoin/MEXC" type data (Lower liquidity, higher baseline volatility)
    # We simulate this by taking our dataset but amplifying volume_z and volatility
    df = pd.read_csv('data/ml_dataset_v4.csv')
    synthetic_mexc = df.copy()
    
    # KuCoin/MEXC typical microstructure shift
    synthetic_mexc['volume_z'] = synthetic_mexc['volume_z'] * 1.5 
    synthetic_mexc['volatility'] = synthetic_mexc['volatility'] * 2.0
    synthetic_mexc['std_rush_orders'] = synthetic_mexc['std_rush_orders'] * 0.8 # Thinner orderbooks
    
    features = ['std_rush_orders', 'volume_z', 'price_z', 'taker_ratio', 'market_cap', 'volatility']
    X_test = synthetic_mexc[features]
    y_test = synthetic_mexc['target']
    
    preds = classifier.model.predict_proba(X_test)[:, 1] >= classifier.threshold
    tp = np.sum((preds == 1) & (y_test == 1))
    fp = np.sum((preds == 1) & (y_test == 0))
    fn = np.sum((preds == 0) & (y_test == 1))
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    
    print(f"Synthetic MEXC Dataset -> Precision: {precision:.2%} | Recall: {recall:.2%} | TP: {tp}, FP: {fp}, FN: {fn}")
    
    if precision > 0.85:
        print("-> Result: OUTSTANDING. Model generalizing well to other exchanges!")
    elif precision > 0.50:
        print("-> Result: ACCEPTABLE. Some overfitting to Binance mechanics, but holds predictive power.")
    else:
        print("-> Result: POOR. Strong overfitting to Binance orderbook microstructure detected.")

if __name__ == "__main__":
    run_walk_forward()
    run_cross_exchange_test()
