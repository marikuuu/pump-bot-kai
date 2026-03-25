import asyncio
import os
import itertools
import pandas as pd
from datetime import datetime, timedelta
import sys
import traceback
import logging

# Add current directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.fetch_history import HistoryFetcher
from backtester.multi_engine import MultiTickEngine
from pump_ai.detector import PumpDetector

async def run_optimization():
    print("\n--- Starting Parameter Sensitivity Optimization (Grid Search) ---")
    
    fetcher = None
    try:
        fetcher = HistoryFetcher('binance')
        # symbols = await fetcher.discover_low_cap_futures(limit=10)
        
        # Manually specify some known volatile symbols if discovery takes too long
        symbols = ['PEPE/USDT:USDT', 'SHIB/USDT:USDT', 'SOL/USDT:USDT', 'ADA/USDT:USDT', 'DOGE/USDT:USDT']
        
        # 1. Prepare Data
        engine_data = []
        for s in symbols:
            fnames = fetcher.get_filenames(s)
            
            # Fetch if missing
            if not all(os.path.exists(f) for f in [fnames['ohlcv'], fnames['trades'], fnames['oi']]):
                print(f"Downloading data for {s}...")
                await fetcher.fetch_ohlcv(s, days=2)
                await fetcher.fetch_trades(s, days=0.1)
                await fetcher.fetch_oi_history(s, days=2)

            if all(os.path.exists(f) for f in [fnames['ohlcv'], fnames['trades'], fnames['oi']]):
                ohlcv = pd.read_csv(fnames['ohlcv'])
                trades = pd.read_csv(fnames['trades'])
                oi = pd.read_csv(fnames['oi']) if os.path.exists(fnames['oi']) else None
                engine_data.append((s, trades, ohlcv, oi))

        if not engine_data:
            print("No data available for optimization.")
            return

        # 2. Define Parameter Grid (Targeting Sensitivity)
        grid = {
            'std_rush': [3.0, 5.0, 8.0, 12.0],
            'oi_z': [1.2, 2.0, 3.0],
            'vol_z': [1.5, 2.5, 4.0]
        }
        
        keys = grid.keys()
        combinations = list(itertools.product(*grid.values()))
        summary = []
        
        print(f"Testing {len(combinations)} combinations across {len(engine_data)} symbols...")
        
        for combo in combinations:
            params = dict(zip(keys, combo))
            detector = PumpDetector(thresholds=params)
            engine = MultiTickEngine(detector)
            
            for s, t, o, i_df in engine_data:
                engine.add_symbol_data(s, t, o, i_df)
                
            # Run Simulation window based on available trades
            all_trade_starts = [state['trades']['timestamp_dt'].min() for state in engine.symbol_states.values()]
            all_trade_ends = [state['trades']['timestamp_dt'].max() for state in engine.symbol_states.values()]
            start_sim = max(all_trade_starts)
            end_sim = min(all_trade_ends)
            
            # Disable logging for faster grid search
            logging.getLogger().setLevel(logging.ERROR)
            engine.run(start_sim, end_sim, step_sec=30)
            logging.getLogger().setLevel(logging.INFO)
            
            # Results
            signals = len(engine.results)
            precision = pd.DataFrame(engine.results)['success'].mean() if signals > 0 else 0.0
            
            summary.append({
                **params,
                'signals': signals,
                'precision': precision
            })
            print(f"Trial: {params} | Signals: {signals} | Precision: {precision:.2%}")

        # 4. Find Winners
        df_results = pd.DataFrame(summary)
        winners = df_results[df_results['precision'] >= 0.90].sort_values('signals', ascending=False)
        
        print("\n--- Optimization Results (Precision >= 90%) ---")
        if not winners.empty:
            print(winners.head(10))
            best = winners.iloc[0].to_dict()
            print(f"\n>>> BEST SENSITIVITY FOR 90% PRECISION: {best} <<<")
        else:
            print("No combination reached 90% precision. System is currently too conservative or data lacks true pumps.")

    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
    finally:
        if fetcher:
            await fetcher.close()

if __name__ == "__main__":
    asyncio.run(run_optimization())
