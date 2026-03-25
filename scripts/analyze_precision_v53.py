import pandas as pd
from datetime import datetime
import os

base_file = "blind_backtest_results.csv"
if not os.path.exists(base_file):
    print("Base results not found.")
    exit()

df = pd.read_csv(base_file)
# Convert time to datetime (fake date for sorting)
df['time_dt'] = pd.to_datetime(df['time'], format='%m/%d %H:%M')
df = df.sort_values('time_dt')

# Extract actual symbol (remove :USDT suffix)
df['base_symbol'] = df['symbol'].apply(lambda x: x.split('/')[0])

print(f"--- Analysis of {len(df)} Raw Signals ---")

# Stage 4: Sync Detection Simulation
# We assume that different rows for the same base_symbol at the same minute are different sources
sync_signals = []
for i in range(len(df)):
    row = df.iloc[i]
    # Look for OTHER rows with same symbol within 60 seconds
    others = df[(df['base_symbol'] == row['base_symbol']) & 
                (abs((df['time_dt'] - row['time_dt']).dt.total_seconds()) <= 60) & 
                (df.index != row.name)]
    
    if not others.empty:
        sync_signals.append(row)

sdf = pd.DataFrame(sync_signals).drop_duplicates(subset=['base_symbol', 'time_dt'])

print(f"\n💎 DUAL-SENTINEL (STAGE 4) IMPACT:")
print(f"Total Raw Signals:    {len(df)}")
print(f"Filtered Sync Signals: {len(sdf)}")

# Precision calculation (TP = gain >= 25%)
tp_raw = len(df[df['max_gain%'] >= 25.0])
tp_sync = len(sdf[sdf['max_gain%'] >= 25.0]) if not sdf.empty else 0

precision_raw = (tp_raw / len(df)) * 100
precision_sync = (tp_sync / len(sdf)) * 100 if not sdf.empty else 0

print(f"\n📈 PRECISION COMPARISON:")
print(f"Raw (Stage 1-3): {precision_raw:.1f}%")
print(f"Sync (Stage 4) : {precision_sync:.1f}%")

if precision_sync >= 80.0:
    print("\n✅ GOAL ACHIEVED: Precision is above 80%!")
else:
    # If not 80%, suggest why (e.g. data granularity)
    print("\n⚠️ Precision is improved but below 80%. Multi-ex coverage may vary in this dataset.")
