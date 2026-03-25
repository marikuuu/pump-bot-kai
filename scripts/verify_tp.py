import asyncio
import os
import pandas as pd
from datetime import datetime, timedelta
import sys
import logging
import traceback

# Add current directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.fetch_history import HistoryFetcher
from backtester.multi_engine import MultiTickEngine
from pump_ai.detector import PumpDetector

async def verify_tp():
    print("\n--- TRUE POSITIVE VERIFICATION: TAC/USDT:USDT (MEXC) ---")
    
    symbol = 'STPT/USDT:USDT'
    # The recent scanner found a pump at 2026-03-22 17:35:00 (UTC)
    event_time = datetime.fromisoformat('2026-03-22T17:35:00+00:00')
    
    fetcher = None
    try:
        fetcher = HistoryFetcher('mexc')
        fnames = fetcher.get_filenames(symbol)
        
        # 1. Ensure Data exists for MEXC
        abs_ohlcv = os.path.abspath(fnames['ohlcv'])
        abs_trades = os.path.abspath(fnames['trades'])
        abs_oi = os.path.abspath(fnames['oi'])
        
        if not os.path.exists(abs_ohlcv):
            print(f"Downloading historical data for {symbol} on MEXC...")
            await fetcher.fetch_ohlcv(symbol, days=3)
            await fetcher.fetch_trades(symbol, days=3)
            await fetcher.fetch_oi_history(symbol, days=3)
        
        # 2. Load Data
        ohlcv = pd.read_csv(abs_ohlcv)
        trades = pd.read_csv(abs_trades)
        oi = pd.read_csv(abs_oi) if os.path.exists(abs_oi) else None
        
        # 3. Setup Detector (Roadmap Settings - Sensitive for verification)
        # We want to see WHEN it triggers.
        detector = PumpDetector(thresholds={'std_rush': 10.0, 'oi_z': 2.0, 'vol_z': 3.0})
        
        # 4. Setup Engine
        engine = MultiTickEngine(detector, target_profit=0.03, target_window_m=15)
        engine.add_symbol_data(symbol, trades, ohlcv, oi)
        
        # 5. Simulation Window (Focus on the event +/- 60 mins)
        start_sim = event_time - timedelta(minutes=60)
        end_sim = event_time + timedelta(minutes=60)
        
        print(f"Replaying Timeline: {start_sim} to {end_sim}")
        logging.getLogger().setLevel(logging.INFO)
        engine.run(start_sim, end_sim, step_sec=30)
        
        # 6. Report Timing
        if engine.results:
            # Sort results by time
            df_res = pd.DataFrame(engine.results).sort_values('time')
            first_signal = df_res.iloc[0]
            detect_time = pd.to_datetime(first_signal['time']).tz_convert('UTC')
            
            print(f"\n✅ SIGNAL TRIGGERED SUCCESSFULLY!")
            print(f"Time of Detection: {detect_time}")
            print(f"Metrics: StdRush={first_signal['std_rush']:.2f}, OI_Z={first_signal['oi_z']:.2f}")
            print(f"Ex-Post Profit (15m): {first_signal['profit']:.2%}")
            
            # Find the peak price in the window to show timing
            peak_ohlcv = ohlcv[(pd.to_datetime(ohlcv['time'], utc=True) >= start_sim) & 
                               (pd.to_datetime(ohlcv['time'], utc=True) <= end_sim)]
            peak_time = pd.to_datetime(peak_ohlcv.loc[peak_ohlcv['high'].idxmax()]['time']).tz_convert('UTC')
            lead_time = (peak_time - detect_time).total_seconds() / 60
            
            print(f"Time of Price Peak: {peak_time}")
            print(f">>> LEAD TIME (Detection to Peak): {lead_time:.1f} minutes <<<")
        else:
            print("\n❌ NO SIGNAL TRIGGERED. Analyzing peak metrics in window:")
            # Find max metrics manually for the user
            all_features = []
            for t_step in pd.date_range(start_sim, end_sim, freq='30s', tz='UTC'):
                # Simulate feature calc for this step
                window_h = ohlcv[(pd.to_datetime(ohlcv['time'], utc=True) >= t_step - timedelta(hours=1)) & 
                                  (pd.to_datetime(ohlcv['time'], utc=True) <= t_step)]
                chunk_t = trades[(pd.to_datetime(trades['timestamp'], unit='ms', utc=True) > t_step - timedelta(seconds=30)) & 
                                 (pd.to_datetime(trades['timestamp'], unit='ms', utc=True) <= t_step)]
                
                if not chunk_t.empty and not window_h.empty:
                    chunk_t['sec'] = pd.to_datetime(chunk_t['timestamp'], unit='ms', utc=True).dt.second
                    rush = chunk_t[chunk_t['side'] == 'buy'].groupby('sec').size().reindex(range(0, 60), fill_value=0).std()
                    vol_z = (chunk_t['amount'].sum() - window_h['volume'].mean()) / (window_h['volume'].std() or 1e-6)
                    all_features.append({'time': t_step, 'std_rush': rush, 'vol_z': vol_z})
            
            df_feat = pd.DataFrame(all_features)
            print(f"Max StdRush seen: {df_feat['std_rush'].max():.2f}")
            print(f"Max Vol_Z seen: {df_feat['vol_z'].max():.2f}")
            print(f"Peak Price Time (Target): {event_time}")
            print(f"Top 5 StdRush moments:\n{df_feat.sort_values('std_rush', ascending=False).head(5)}")

    except Exception as e:
        print(f"FAILED: {e}")
        traceback.print_exc()
    finally:
        if fetcher:
            await fetcher.close()

if __name__ == "__main__":
    asyncio.run(verify_tp())
