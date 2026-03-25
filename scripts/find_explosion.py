import asyncio
import os
import pandas as pd
from datetime import datetime, timedelta
import sys
import logging

# Add current directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.fetch_history import HistoryFetcher

async def find_real_explosion():
    print("\n--- DEEP SCAN: Real Rank 100-1500 Explosion Search ---")
    
    # Try Binance primarily for trade history stability
    for exch_id in ['binance']:
        print(f"\nScanning {exch_id.upper()}...")
        fetcher = HistoryFetcher(exch_id)
        
        # Discover a lot of symbols
        symbols = await fetcher.discover_low_cap_futures(limit=40)
        
        for s in symbols:
            try:
                # Fetch 1m candles for 2 days
                df = await fetcher.fetch_ohlcv(s, days=2)
                if df is None or df.empty: continue
                
                # Check for "Explosion": >8% move in 15 mins
                df['future_high'] = df['high'].rolling(window=15).max().shift(-15)
                df['pump'] = (df['future_high'] - df['close']) / df['close']
                
                explosions = df[df['pump'] > 0.08].sort_values('pump', ascending=False)
                if not explosions.empty:
                    event = explosions.iloc[0]
                    print(f"!!! [REAL EXPLOSION FOUND] !!!")
                    print(f"Symbol: {s} on {exch_id.upper()}")
                    print(f"Time: {event['time']}")
                    print(f"Expected Gain: {event['pump']:.2%}")
                    
                    # Store event details
                    with open("last_explosion.txt", "w") as f:
                        f.write(f"{exch_id},{s},{event['time']}")
                    
                    await fetcher.close()
                    return # Exit after finding one good one
            except Exception as e:
                continue
        
        await fetcher.close()
    
    print("\nNo major explosions (>8%) found in the last 2 days.")

if __name__ == "__main__":
    asyncio.run(find_real_explosion())
