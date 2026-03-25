import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pump_ai.pump_classifier import PumpClassifier

df = pd.read_csv('data/ml_dataset_v4.csv')
features = ['std_rush_orders', 'volume_z', 'price_z', 'taker_ratio', 'market_cap', 'volatility', 
            'price_momentum_3h', 'volume_momentum_3h', 'avg_trade_size', 'max_trade_size']
X = df[features]
y = df['target']

split_idx = int(len(df) * 0.8)
X_val, y_val = X.iloc[split_idx:], y.iloc[split_idx:]

classifier = PumpClassifier()
if not classifier.load_model():
    print("Models not found!")
    exit(1)

# Test on the ENTIRE dataset to see total TP/FP separability
print("Evaluating precision on the ENTIRE dataset...")
classifier.tune_threshold(X, y, target_precision=0.80)
