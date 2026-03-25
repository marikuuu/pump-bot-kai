import sys
import os
import asyncio
import argparse
import pandas as pd
from datetime import datetime, timedelta, timezone
import traceback

# Add current directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.fetch_history import HistoryFetcher
from backtester.multi_engine import MultiTickEngine
from pump_ai.detector import PumpDetector

async def main():
    parser = argparse.ArgumentParser(description='Run Multi-Symbol Full-High-Fidelity Backtest')
    parser.add_argument('--exchange', type=str, default='binance', help='Exchange ID')
    parser.add_argument('--limit', type=int, default=5, help='Number of symbols to simulate')
    parser.add_argument('--days', type=int, default=1, help='OHLCV history (Baseline)')
    args = parser.parse_args()

    fetcher = None
    try:
        fetcher = HistoryFetcher(args.exchange)
        detector = PumpDetector()
        engine = MultiTickEngine(detector)

        # 1. Discover Low-Cap Futures
        symbols = await fetcher.discover_low_cap_futures(limit=args.limit)
        print(f"\n--- Starting Multi-Symbol Stress Test for {len(symbols)} tickers ---")
        
        # 2. Collect Data
        for symbol in symbols:
            fnames = fetcher.get_filenames(symbol)
            
            # Fetch if missing
            if not all(os.path.exists(f) for f in [fnames['ohlcv'], fnames['trades'], fnames['oi']]):
                print(f"Downloading data for {symbol}...")
                await fetcher.fetch_ohlcv(symbol, days=args.days)
                await fetcher.fetch_trades(symbol, days=0.1) # Max possible historical trades
                await fetcher.fetch_oi_history(symbol, days=args.days)
            
            # Load
            ohlcv_df = pd.read_csv(fnames['ohlcv'])
            trades_df = pd.read_csv(fnames['trades'])
            oi_df = pd.read_csv(fnames['oi']) if os.path.exists(fnames['oi']) else None
            
            engine.add_symbol_data(symbol, trades_df, ohlcv_df, oi_df)

        # 3. Determine Replay Window
        # Simulation period is limited by the trade data availability (usually very recent)
        all_trade_starts = [state['trades']['timestamp_dt'].min() for state in engine.symbol_states.values()]
        all_trade_ends = [state['trades']['timestamp_dt'].max() for state in engine.symbol_states.values()]
        
        start_sim = max(all_trade_starts)
        end_sim = min(all_trade_ends)
        
        print(f"Replaying Timeline: {start_sim} to {end_sim}")

        # 4. Run Stress Test
        engine.run(start_sim, end_sim, step_sec=30)
        
        # 5. Report
        engine.report()
        
    except Exception as e:
        print(f"\n!!! ERROR IN MULTI-BACKTEST: {e} !!!")
        traceback.print_exc()
    finally:
        if fetcher:
            await fetcher.close()

if __name__ == "__main__":
    asyncio.run(main())
