import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import os
import sys

# Add current directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.fetch_history import HistoryFetcher
from pump_ai.detector import PumpDetector

async def analyze_br():
    symbol = 'BR/USDT:USDT'
    fetcher = HistoryFetcher('mexc')
    fnames = fetcher.get_filenames(symbol)
    
    ohlcv = pd.read_csv(fnames['ohlcv'])
    trades = pd.read_csv(fnames['trades'])
    
    ohlcv['time_dt'] = pd.to_datetime(ohlcv['time'], utc=True)
    trades['ts_dt'] = pd.to_datetime(trades['timestamp'], unit='ms', utc=True)
    
    # Detector (Balanced)
    detector = PumpDetector(thresholds={'std_rush': 6.5, 'oi_z': 2.0, 'vol_z': 2.0})
    
    print(f"\n--- EXPLOSION ANALYSIS: {symbol} ---")
    
    # Scan every 30s in the last 24h
    start_ana = ohlcv['time_dt'].min()
    end_ana = ohlcv['time_dt'].max()
    
    steps = pd.date_range(start_ana, end_ana, freq='30s', tz='UTC')
    found = False
    for t in steps:
        chunk = trades[(trades['ts_dt'] > t - timedelta(seconds=30)) & (trades['ts_dt'] <= t)]
        if chunk.empty: continue
        
        # Rush Calculation
        chunk_df = chunk.copy()
        chunk_df['sec'] = chunk_df['ts_dt'].dt.second
        rush = chunk_df[chunk_df['side'] == 'buy'].groupby('sec').size().reindex(range(0, 60), fill_value=0).std()
        
        # Metrics for Detector
        window = ohlcv[ohlcv['time_dt'] <= t].tail(60)
        vol_z = (chunk['amount'].sum() - window['volume'].mean()) / (window['volume'].std() or 1e-6)
        price_z = (chunk['price'].iloc[-1] - window['close'].mean()) / (window['close'].std() or 1e-6)
        
        data = {'std_rush_orders': rush, 'vol_z': vol_z, 'price_z': price_z, 'symbol': symbol}
        is_pump, score, stage = detector.check_event(data)
        
        if is_pump and not found:
            found = True
            detect_price = chunk['price'].iloc[-1]
            print(f"!!! [GOD SIGNAL] TRIGGERED !!!")
            print(f"Time:  {t}")
            print(f"Price: {detect_price:.6f}")
            print(f"Rush:  {rush:.2f}")
            
    if not found:
        print("No signal triggered with StdRush=6.5.")
        # Print Peak Rush
        max_rush = 0
        for t in steps:
            # (same rush calc)
            pass
        # print(...)

    await fetcher.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(analyze_br())
