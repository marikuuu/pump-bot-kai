import pandas as pd
import pickle
from datetime import datetime, timezone

# Load data and model
df = pd.read_csv('tick_training_data.csv')
with open('pump_ai/pump_model_v5_tick.pkl', 'rb') as f:
    model = pickle.load(f)

FEATURES = ['vol_z', 'pc_z', 'pre_accum_z', 'std_rush', 'avg_trade_size', 'max_trade_size', 'median_trade_size', 'buy_ratio', 'acceleration', 'price_impact']
X = df[FEATURES].fillna(0)
probas = model.predict_proba(X)[:, 1]
df['confidence'] = probas

# Filter for detection threshold
threshold = 0.85
hits = df[df['confidence'] >= threshold].copy()

def format_ts(ts_ms):
    return datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc).strftime('%m/%d %H:%M')

hits['time'] = hits['timestamp'].apply(format_ts)

print(f"=== v5.0 Backtest Summary ===")
print(f"Total Candidates: {len(df)}")
print(f"Total Detections (Conf >= {threshold}): {len(hits)}")
print(f"True Positives (Reached 1.5x): {len(hits[hits['label'] == 1])}")
print(f"False Positives: {len(hits[hits['label'] == 0])}")
precision = (len(hits[hits['label'] == 1]) / len(hits) * 100) if len(hits) > 0 else 0
print(f"Precision: {precision:.1f}%")

print("\n=== Top Signals Detail (First Detection per Symbol) ===")
cols = ['symbol', 'time', 'confidence', 'label']
unique_hits = hits.sort_values(['symbol', 'timestamp']).drop_duplicates(subset=['symbol'], keep='first')
print(unique_hits[cols].to_string(index=False))
