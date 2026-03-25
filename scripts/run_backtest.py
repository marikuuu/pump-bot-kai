import sys
import os
import asyncio
import argparse
import pandas as pd
import traceback

# Add current directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.fetch_history import HistoryFetcher
from backtester.engine import BacktestEngine
from pump_ai.detector import PumpDetector

async def main():
    parser = argparse.ArgumentParser(description='Run Pump Detection Backtest (Futures/OI)')
    parser.add_argument('--exchange', type=str, default='binance', help='Exchange ID')
    parser.add_argument('--symbol', type=str, default='BTC/USDT:USDT', help='Symbol (e.g. BTC/USDT:USDT)')
    parser.add_argument('--days', type=int, default=7, help='Days of history')
    args = parser.parse_args()

    fetcher = None
    try:
        fetcher = HistoryFetcher(args.exchange)
        detector = PumpDetector()
        engine = BacktestEngine(detector)

        print(f"\n--- Initializing Futures Backtest for {args.symbol} on {args.exchange} ---")
        
        # 1. Fetch/Load Data
        fnames = fetcher.get_filenames(args.symbol)
        
        if not all(os.path.exists(f) for f in [fnames['ohlcv'], fnames['trades'], fnames['oi']]):
            print("Data files missing or incomplete. Fetching from exchange...")
            await fetcher.fetch_ohlcv(args.symbol, days=args.days)
            await fetcher.fetch_trades(args.symbol, days=min(args.days, 1))
            await fetcher.fetch_oi_history(args.symbol, days=args.days)
        
        # Load into memory
        print(f"Loading OHLCV: {fnames['ohlcv']}")
        ohlcv_df = pd.read_csv(fnames['ohlcv'])
        ohlcv_df['time'] = pd.to_datetime(ohlcv_df['time'], utc=True)
            
        print(f"Loading Trades: {fnames['trades']}")
        trades_df = pd.read_csv(fnames['trades'])
        
        oi_df = None
        if os.path.exists(fnames['oi']):
            print(f"Loading OI: {fnames['oi']}")
            oi_df = pd.read_csv(fnames['oi'])
            oi_df['time'] = pd.to_datetime(oi_df['time'], utc=True)
        
        # 2. Run Engine
        engine.run(trades_df, ohlcv_df, oi_df)
        
        # 3. Report
        engine.report()
        
    except Exception as e:
        print(f"\n!!! ERROR IN BACKTEST: {e} !!!")
        traceback.print_exc()
    finally:
        if fetcher:
            await fetcher.close()

if __name__ == "__main__":
    asyncio.run(main())
