import pandas as pd
import numpy as np
import os
import sys

# Standardized Event List
events = [
    {"symbol": "IRUSDT", "date": "2026-03-16", "jst": "10:00", "ts": 1773622800000},
    {"symbol": "AIAUSDT", "date": "2026-03-17", "jst": "12:00", "ts": 1773716400000},
    {"symbol": "PTBUSDT", "date": "2026-03-18", "jst": "17:00", "ts": 1773820800000},
    {"symbol": "RDNTUSDT", "date": "2026-03-19", "jst": "00:00", "ts": 1773846000000},
    {"symbol": "BRUSDT", "date": "2026-03-19", "jst": "18:00", "ts": 1773910800000},
    {"symbol": "MAGMAUSDT", "date": "2026-03-19", "jst": "04:00", "ts": 1773860400000},
    {"symbol": "GUNUSDT", "date": "2026-03-20", "jst": "07:00", "ts": 1773957600000},
    {"symbol": "A2ZUSDT", "date": "2026-03-20", "jst": "12:00", "ts": 1773975600000},
    {"symbol": "BTRUSDT", "date": "2026-03-20", "jst": "01:00", "ts": 1773936000000},
    {"symbol": "LIGHTUSDT", "date": "2026-03-20", "jst": "01:00", "ts": 1773936000000},
    {"symbol": "ONTUSDT", "date": "2026-03-21", "jst": "21:00", "ts": 1774094400000},
    {"symbol": "DUSKUSDT", "date": "2026-03-21", "jst": "17:00", "ts": 1774080000000},
]

def run_backtest_for_event(event):
    symbol = event["symbol"]
    date = event["date"]
    target_ts = event["ts"]
    
    bin_file = f"data/{symbol}_binance_{date}.csv"
    byb_file = f"data/{symbol}_{date}.csv"
    
    print(f"\n>>> Analyzing {symbol} on {date} (Target: {event['jst']} JST) <<<")
    
    # Load data
    dfs = []
    if os.path.exists(bin_file):
        dfb = pd.read_csv(bin_file)
        dfb['ts_ms'] = dfb['transact_time']
        dfb['exchange'] = 'Binance'
        dfs.append(dfb[['ts_ms', 'price', 'exchange']])
    
    if os.path.exists(byb_file):
        dfy = pd.read_csv(byb_file)
        dfy['ts_ms'] = (dfy['timestamp'] * 1000).astype(np.int64)
        dfy['exchange'] = 'Bybit'
        dfs.append(dfy[['ts_ms', 'price', 'exchange']])
    
    if not dfs:
        print(f"No data found for {symbol} on {date}")
        return None

    all_ticks = pd.concat(dfs).sort_values('ts_ms')
    
    # Analysis Window: Target TS +/- 15 minutes
    window = 900000
    w_ticks = all_ticks[(all_ticks['ts_ms'] >= target_ts - window) & (all_ticks['ts_ms'] <= target_ts + window)]
    
    if w_ticks.empty:
        print(f"No ticks in window for {symbol}")
        return None

    # Detect first move > 1% (v5.1 relaxed criteria) in a 1-minute window
    # Actually let's just find the max gain in the 30min window
    p_min = w_ticks['price'].min()
    p_max = w_ticks['price'].max()
    gain = (p_max / p_min - 1) * 100
    
    # Lead-Lag Analysis if both exist
    lead_lag = "N/A"
    if 'Binance' in w_ticks['exchange'].values and 'Bybit' in w_ticks['exchange'].values:
        # Simplistic trigger: first touch of (p_min * 1.005)
        trigger_p = p_min * 1.005
        t_bin = w_ticks[(w_ticks['exchange'] == 'Binance') & (w_ticks['price'] >= trigger_p)]['ts_ms'].min()
        t_byb = w_ticks[(w_ticks['exchange'] == 'Bybit') & (w_ticks['price'] >= trigger_p)]['ts_ms'].min()
        
        if pd.notnull(t_bin) and pd.notnull(t_byb):
            diff = t_byb - t_bin
            if diff > 0: lead_lag = f"Binance LED Bybit by {diff}ms"
            elif diff < 0: lead_lag = f"Bybit LED Binance by {abs(diff)}ms"
            else: lead_lag = "Perfect Sync"

    print(f"Result: Max Gain in Window: {gain:.2f}% | Lead-Lag: {lead_lag}")
    return {"symbol": symbol, "gain": gain, "lead_lag": lead_lag}

summary = []
for e in events:
    res = run_backtest_for_event(e)
    if res: summary.append(res)

print("\n=== V5.4 FULL BACKTEST SUMMARY (3/16-3/21) ===")
sdf = pd.DataFrame(summary)
print(sdf)
