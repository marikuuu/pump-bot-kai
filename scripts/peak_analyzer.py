import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys

# Add current directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.fetch_history import HistoryFetcher

async def analyze_peak():
    symbol = 'RIF/USDT:USDT'
    event_time = datetime.fromisoformat('2026-03-22T03:07:00+00:00')
    fetcher = HistoryFetcher('mexc')
    fnames = fetcher.get_filenames(symbol)
    
    trades = pd.read_csv(fnames['trades'])
    ohlcv = pd.read_csv(fnames['ohlcv'])
    ohlcv['time_dt'] = pd.to_datetime(ohlcv['time'], utc=True)
    trades['ts_dt'] = pd.to_datetime(trades['timestamp'], unit='ms', utc=True)
    
    start_ana = event_time - timedelta(minutes=15)
    end_ana = event_time + timedelta(minutes=15)
    
    print(f"\n--- PEAK ANALYZER: {symbol} around {event_time} ---")
    
    steps = pd.date_range(start_ana, end_ana, freq='30s', tz='UTC')
    results = []
    for t in steps:
        chunk = trades[(trades['ts_dt'] > t - timedelta(seconds=30)) & (trades['ts_dt'] <= t)]
        window = ohlcv[(ohlcv['time_dt'] > t - timedelta(hours=1)) & (ohlcv['time_dt'] <= t)]
        
        if chunk.empty or window.empty: continue
        
        # Rush Calculation
        chunk['sec'] = chunk['ts_dt'].dt.second
        rush = chunk[chunk['side'] == 'buy'].groupby('sec').size().reindex(range(0, 60), fill_value=0).std()
        vol_z = (chunk['amount'].sum() - window['volume'].mean()) / (window['volume'].std() or 1e-6)
        price = chunk['price'].iloc[-1]
        
        results.append({'time': t, 'rush': rush, 'vol_z': vol_z, 'price': price})
    
    df = pd.DataFrame(results)
    if not df.empty:
        print(df.sort_values('rush', ascending=False).head(10).to_string(index=False))
        max_rush = df['rush'].max()
        print(f"\nPEAK RUSH DETECTED: {max_rush:.2f}")
    else:
        print("No trades found in the window.")

    await fetcher.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(analyze_peak())
