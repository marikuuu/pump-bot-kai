import pandas as pd
import json
from datetime import datetime, timezone
import pytz
import os
import sys

sys.path.append(os.path.abspath('.'))
from pump_ai.pump_classifier import PumpClassifier

df = pd.read_csv('data/ml_dataset_v4.csv')
features = ['std_rush_orders', 'volume_z', 'price_z', 'taker_ratio', 'market_cap', 'volatility']
X = df[features]
y = df['target']

classifier = PumpClassifier()
classifier.load_model()
classifier.tune_threshold(X, y, target_precision=0.90)

unanimous_preds = classifier.predict(X)

tp_indices = df[(unanimous_preds == 1) & (y == 1)].index

with open('multiverse_candidates.json', 'r') as f:
    candidates = json.load(f)

# Ensure types match
timestamp_to_symbol = {int(c['start_ts']): c['symbol'] for c in candidates}
jst = pytz.timezone('Asia/Tokyo')

for idx in tp_indices:
    row = df.iloc[idx]
    ts = int(row['timestamp'])
    dt_utc = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
    dt_jst = dt_utc.astimezone(jst)
    
    symbol = timestamp_to_symbol.get(ts, 'Unknown')
    print(f'Symbol: {symbol} | Detected Base: {dt_jst.strftime("%Y-%m-%d %H:%M:%S")} (JST) | Rush Std: {row["std_rush_orders"]:.2f} | Vol Spike (Z): {row["volume_z"]:.2f}')
