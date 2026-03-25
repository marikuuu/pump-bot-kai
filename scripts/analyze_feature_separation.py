import pandas as pd
import numpy as np

df = pd.read_csv('data/ml_dataset_v4.csv')
pumps = df[df['target'] == 1]
noise = df[df['target'] == 0]

print(f"Pumps: {len(pumps)} | Noise: {len(noise)}")

features = ['std_rush_orders', 'volume_z', 'price_z', 'taker_ratio', 'volatility', 'price_momentum_3h', 'volume_momentum_3h']

print("\n--- FEATURE MEANS ---")
print(f"{'Feature':<20} | {'Pumps':<10} | {'Noise':<10} | {'Ratio':<10}")
print("-" * 55)
for f in features:
    m_p = pumps[f].mean()
    m_n = noise[f].mean()
    ratio = m_p / m_n if m_n != 0 else np.nan
    print(f"{f:<20} | {m_p:<10.4f} | {m_n:<10.4f} | {ratio:<10.2f}")

print("\n--- FEATURE MAXES ---")
for f in features:
    print(f"{f:<20} | Pumps Max: {pumps[f].max():.2f} | Noise Max: {noise[f].max():.2f}")
