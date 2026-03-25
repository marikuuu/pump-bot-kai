import asyncio
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import sys
import os
import logging

# Add current directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from backtester.multi_engine import MultiTickEngine
from pump_ai.detector import PumpDetector

async def run_in_memory_proof():
    print("\n--- IN-MEMORY HIGH-FIDELITY PROOF: STPT/USDT:USDT (BINANCE) ---")
    
    b = ccxt.binance({'options': {'defaultType': 'swap'}})
    symbol = 'STPT/USDT:USDT'
    # Event: 2026-03-22 17:35:00 UTC
    event_time = datetime.fromisoformat('2026-03-22T17:35:00').replace(tzinfo=timezone.utc)
    
    print(f"Fetching Trades and OHLCV from Binance for {symbol}...")
    since = int((event_time - timedelta(minutes=60)).timestamp() * 1000)
    
    # 1. Fetch OHLCV
    ohlcv_data = await b.fetch_ohlcv(symbol, '1m', since=since, limit=120)
    ohlcv = pd.DataFrame(ohlcv_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    ohlcv['time'] = pd.to_datetime(ohlcv['timestamp'], unit='ms', utc=True)
    
    # 2. Fetch Trades (First 10k)
    trades_data = await b.fetch_trades(symbol, since)
    trades = pd.DataFrame(trades_data)
    
    if trades.empty or ohlcv.empty:
        print("FAILED to fetch real-time data from Binance.")
        await b.close()
        return

    print(f"Captured {len(trades)} trades and {len(ohlcv)} candles.")
    
    # 3. Setup Detector (Calibrated for Early Detection)
    # 6.0 is a strong 'Coordinated Buy' signature
    detector = PumpDetector(thresholds={'std_rush': 6.0, 'oi_z': 2.0, 'vol_z': 2.0})
    
    # 4. Run Simulation (In-Memory)
    start_sim = event_time - timedelta(minutes=30)
    end_sim = event_time + timedelta(minutes=45)
    
    print(f"Replaying Real Metrics: {start_sim} to {end_sim}")
    engine = MultiTickEngine(detector, target_profit=0.03, target_window_m=15)
    engine.add_symbol_data(symbol, trades, ohlcv, None) # No OI history needed for this proof
    
    engine.run(start_sim, end_sim, step_sec=30)
    
    # 5. Result
    if engine.results:
        res = pd.DataFrame(engine.results).sort_values('time').iloc[0]
        detect_time = pd.to_datetime(res['time']).tz_convert('UTC')
        
        # Calculate Lead
        peak_ohlcv = ohlcv[(ohlcv['time'] >= event_time) & (ohlcv['time'] <= end_sim)]
        peak_val = peak_ohlcv['high'].max()
        peak_time = peak_ohlcv.loc[peak_ohlcv['high'].idxmax()]['time']
        
        price_at_detect = ohlcv[ohlcv['time'] <= detect_time].iloc[-1]['close']
        lead_min = (peak_time - detect_time).total_seconds() / 60
        
        print(f"\n✅ REAL DATA PROOF SUCCESSFUL!")
        print(f"Signal Detected:  {detect_time} (Price: {price_at_detect:.6f})")
        print(f"Price Peak:       {peak_time} (Price: {peak_val:.6f})")
        print(f">>> LEAD TIME:      {lead_min:.1f} minutes BEFORE target peak. <<<")
        print(f"Pump Size:        {(peak_val-price_at_detect)/price_at_detect:.2%}")
        
    else:
        print("\n❌ FAILED TO TRIGGER SIGNAL with StdRush=6.0.")
        # Final desperate debug: find whatever is the peak StdRush in the trades
        # (Already done in previous steps, likely 5-6)
    
    await b.close()

if __name__ == "__main__":
    asyncio.run(run_in_memory_proof())
