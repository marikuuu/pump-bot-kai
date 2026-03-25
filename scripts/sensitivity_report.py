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

async def generate_report():
    print("\n--- HIGH-FIDELITY SENSITIVITY REPORT: ADA & SOL ---")
    
    symbols = ['ADA/USDT:USDT', 'SOL/USDT:USDT']
    fetcher = None
    try:
        fetcher = HistoryFetcher('binance')
        
        scenarios = [
            {'name': 'Ironclad', 'std_rush': 12.0, 'oi_z': 2.5, 'vol_z': 5.0},
            {'name': 'Roadmap', 'std_rush': 10.0, 'oi_z': 2.0, 'vol_z': 3.0},
            {'name': 'Moderate', 'std_rush': 5.0, 'oi_z': 1.5, 'vol_z': 2.0},
            {'name': 'Sensitive', 'std_rush': 2.0, 'oi_z': 1.0, 'vol_z': 1.0},
        ]
        
        results = []
        for s in scenarios:
            detector = PumpDetector(thresholds={k:v for k,v in s.items() if k != 'name'})
            engine = MultiTickEngine(detector)
            
            for symbol in symbols:
                fnames = fetcher.get_filenames(symbol)
                ohlcv = pd.read_csv(fnames['ohlcv'])
                trades = pd.read_csv(fnames['trades'])
                oi = pd.read_csv(fnames['oi']) if os.path.exists(fnames['oi']) else None
                engine.add_symbol_data(symbol, trades, ohlcv, oi)
            
            all_trade_starts = [state['trades']['timestamp_dt'].min() for state in engine.symbol_states.values()]
            all_trade_ends = [state['trades']['timestamp_dt'].max() for state in engine.symbol_states.values()]
            start_sim = max(all_trade_starts)
            end_sim = min(all_trade_ends)
            
            logging.getLogger().setLevel(logging.ERROR)
            engine.run(start_sim, end_sim, step_sec=30)
            
            signals = len(engine.results)
            precision = pd.DataFrame(engine.results)['success'].mean() if signals > 0 else 0.0
            
            results.append({
                'Mode': s['name'],
                'Rush_Th': s['std_rush'],
                'Signals': signals,
                'Precision': f"{precision:.2%}" if signals > 0 else "N/A"
            })

        print("\n" + pd.DataFrame(results).to_string(index=False))
        
    except Exception as e:
        print(f"FAILED: {e}")
        traceback.print_exc()
    finally:
        if fetcher:
            await fetcher.close()

if __name__ == "__main__":
    asyncio.run(generate_report())
