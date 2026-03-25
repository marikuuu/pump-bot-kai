import asyncio
import os
import pandas as pd
from datetime import datetime, timedelta
import sys

# Add current directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.fetch_history import HistoryFetcher

async def find_pump_window():
    print("\n--- Scanning for Real Pump Events (MEXC) ---")
    fetcher = HistoryFetcher('mexc')
    symbols = await fetcher.discover_low_cap_futures(limit=15)
    
    found_events = []
    for s in symbols:
        print(f"Scanning {s}...")
        df = await fetcher.fetch_ohlcv(s, days=0.5) # Last 12 hours
        if df is None or df.empty: continue
        
        # Look for 3% gain in 5 minutes (more sensitive for recent window)
        df['max_5m'] = df['high'].rolling(window=5).max().shift(-5)
        df['pump_pct'] = (df['max_5m'] - df['close']) / df['close']
        
        pumps = df[df['pump_pct'] > 0.025].sort_values('pump_pct', ascending=False)
        if not pumps.empty:
            best_pump = pumps.iloc[0]
            print(f"!!! FOUND PUMP on {s} at {best_pump['time']} !!! Gain: {best_pump['pump_pct']:.2%}")
            found_events.append({
                'symbol': s,
                'time': best_pump['time'],
                'gain': best_pump['pump_pct']
            })

    if found_events:
        # Fetch high-fidelity trades for the best event
        event = found_events[0]
        # Trade fetching is limited to recent history, so we hope it's recent
        await fetcher.fetch_trades(event['symbol'], days=2)
        await fetcher.fetch_oi_history(event['symbol'], days=2)
        print(f"\n>>> TARGET EVENT: {event['symbol']} at {event['time']} <<<")
    else:
        print("\nNo significant pumps (>4%) found in the last 2 days for the target segment.")

    await fetcher.close()

if __name__ == "__main__":
    asyncio.run(find_pump_window())
