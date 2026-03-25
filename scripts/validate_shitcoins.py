import asyncio
import os
import pandas as pd
from datetime import datetime
import sys
import logging
import traceback

# Add current directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.fetch_history import HistoryFetcher
from backtester.multi_engine import MultiTickEngine
from pump_ai.detector import PumpDetector

async def run_shitcoin_validation():
    print("\n--- SHITCOIN VALIDATION: Rank 100-1500 Market Segment ---")
    
    fetcher = None
    try:
        fetcher = HistoryFetcher('binance')
        # 1. Discover actual low-vol/small-cap futures
        symbols = await fetcher.discover_low_cap_futures(limit=10)
        
        # 2. Scenarios (Balanced Roadmap Settings)
        roadmap_params = {'std_rush': 10.0, 'oi_z': 2.0, 'vol_z': 3.0}
        detector = PumpDetector(thresholds=roadmap_params)
        engine = MultiTickEngine(detector)

        # 3. Collect & Load Data
        for s in symbols:
            fnames = fetcher.get_filenames(s)
            if not all(os.path.exists(f) for f in [fnames['ohlcv'], fnames['trades'], fnames['oi']]):
                print(f"Downloading {s}...")
                await fetcher.fetch_ohlcv(s, days=2)
                await fetcher.fetch_trades(s, days=0.1)
                await fetcher.fetch_oi_history(s, days=2)
            
            if os.path.exists(fnames['ohlcv']):
                ohlcv = pd.read_csv(fnames['ohlcv'])
                trades = pd.read_csv(fnames['trades'])
                oi = pd.read_csv(fnames['oi']) if os.path.exists(fnames['oi']) else None
                engine.add_symbol_data(s, trades, ohlcv, oi)

        # 4. Run Simulation
        if not engine.symbol_states:
            print("No valid small-cap data found.")
            return

        all_trade_starts = [state['trades']['timestamp_dt'].min() for state in engine.symbol_states.values()]
        all_trade_ends = [state['trades']['timestamp_dt'].max() for state in engine.symbol_states.values()]
        start_sim = max(all_trade_starts)
        end_sim = min(all_trade_ends)
        
        print(f"Stress Testing across {len(engine.symbol_states)} small-cap symbols...")
        logging.getLogger().setLevel(logging.ERROR)
        engine.run(start_sim, end_sim, step_sec=30)
        
        # 5. Report
        engine.report()
        
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
    finally:
        if fetcher:
            await fetcher.close()

if __name__ == "__main__":
    asyncio.run(run_shitcoin_validation())
