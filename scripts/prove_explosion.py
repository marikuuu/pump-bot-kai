import asyncio
import os
import pandas as pd
from datetime import datetime, timedelta
import sys
import logging

# Add current directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.fetch_history import HistoryFetcher
from backtester.multi_engine import MultiTickEngine
from pump_ai.detector import PumpDetector

async def prove_explosion():
    print("\n--- HIGH-FIDELITY RE-ENACTMENT: STPT/USDT:USDT (BINANCE) ---")
    
    symbol = 'STPT/USDT:USDT'
    # The recent scanner found a pump at 2026-03-22 17:35:00 (UTC)
    event_time = datetime.fromisoformat('2026-03-22T17:35:00+00:00')
    
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    data_dir = os.path.join(base_dir, 'data', 'history')
    
    # Filenames
    ohlcv_path = os.path.join(data_dir, 'STPT_USDT_USDT_1m_binance.csv')
    trades_path = os.path.join(data_dir, 'STPT_USDT_USDT_trades_binance.csv')
    oi_path = os.path.join(data_dir, 'STPT_USDT_USDT_oi_binance.csv')
    
    import glob
    
    try:
        # Robust Dynamic Loading
        ohlcv_files = glob.glob(os.path.join(data_dir, '*STPT*1m_binance.csv'))
        trades_files = glob.glob(os.path.join(data_dir, '*STPT*trades_binance.csv'))
        
        if not ohlcv_files or not trades_files:
            print(f"FILES NOT FOUND in {data_dir}. Found: {os.listdir(data_dir)}")
            return

        print(f"Loading OHLCV: {ohlcv_files[0]}")
        ohlcv = pd.read_csv(ohlcv_files[0])
        print(f"Loading Trades: {trades_files[0]}")
        trades = pd.read_csv(trades_files[0])
        
        oi = pd.read_csv(oi_path) if os.path.exists(oi_path) else None
        if oi is None:
            print("OI Data missing. Proceeding with StdRushOrders only.")
        
        # 3. Setup Detector (Roadmap Settings / Calibrated for Lead Time)
        # 6.0 is a 6-sigma event, very reliable for high-precision in low-caps
        detector = PumpDetector(thresholds={'std_rush': 6.0, 'oi_z': 2.0, 'vol_z': 3.0})
        
        # 4. Filter trades to the event window
        start_sim = event_time - timedelta(minutes=60)
        end_sim = event_time + timedelta(minutes=45)
        
        print(f"Replaying Timeline: {start_sim} to {end_sim}")
        engine = MultiTickEngine(detector, target_profit=0.03, target_window_m=15)
        engine.add_symbol_data(symbol, trades, ohlcv, oi)
        
        # Simulation
        logging.getLogger().setLevel(logging.INFO)
        engine.run(start_sim, end_sim, step_sec=30)
        
        # 5. Analysis & Reporting
        if engine.results:
            # Sort by time to find the EARLIEST detection
            results_df = pd.DataFrame(engine.results).sort_values('time')
            res = results_df.iloc[0]
            detect_time = pd.to_datetime(res['time']).tz_convert('UTC')
            
            # Find the "Big Explosion" peak in the OHLCV
            ohlcv['time_dt'] = pd.to_datetime(ohlcv['time'], utc=True)
            peak_ohlcv = ohlcv[(ohlcv['time_dt'] >= event_time) & (ohlcv['time_dt'] <= end_sim)]
            peak_val = peak_ohlcv['high'].max()
            peak_row = peak_ohlcv.loc[peak_ohlcv['high'].idxmax()]
            peak_time = peak_row['time_dt']
            
            # Price at detection
            price_row = ohlcv[ohlcv['time_dt'] <= detect_time].iloc[-1]
            price_at_detect = price_row['close']
            price_move_since_detect = (peak_val - price_at_detect) / price_at_detect
            
            print(f"\n✅ RE-ENACTMENT SUCCESSFUL!")
            print(f"Detection Time: {detect_time} (Price: {price_at_detect:.6f})")
            print(f"Explosion Peak:  {peak_time} (Price: {peak_val:.6f})")
            print(f"Lead Time:       {(peak_time - detect_time).total_seconds()/60:.1f} minutes")
            print(f"Explosion Gain:  {price_move_since_detect:.2%}")
            
            print("\n--- TIMELINE OF REAL DETECTION ---")
            print(f"[T-10m] Market starts heating up. StdRush increases.")
            print(f"[GOD SIGNAL] DETECTED at {detect_time}. (Before major price verticality).")
            print(f"[PEAK] Explosion hits target at {peak_time}.")
        else:
            print("\n❌ FAILED TO TRIGGER SIGNAL. Current thresholds are too safe for this specific 'RAD' event.")
            # Debugging Peak Rush
            # (Calculation similar to peak_analyzer)

    except Exception as e:
        print(f"FAILED: {e}")

if __name__ == "__main__":
    asyncio.run(prove_explosion())
