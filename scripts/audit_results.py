import pandas as pd
import os
import sys
from datetime import datetime

# Add current directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.fetch_history import HistoryFetcher

def get_detailed_log():
    fetcher = HistoryFetcher('binance')
    symbols = ['WAVES/USDT:USDT', 'KAVA/USDT:USDT', 'BAND/USDT:USDT', 'STPT/USDT:USDT', 
               'GTC/USDT:USDT', 'CVX/USDT:USDT', 'HFT/USDT:USDT', 'RAD/USDT:USDT', 
               'MTL/USDT:USDT', 'VET/USDT:USDT']
    
    report = []
    for s in symbols:
        f = fetcher.get_filenames(s)
        if os.path.exists(f['trades']):
            df = pd.read_csv(f['trades'])
            df['ts'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            start = df['ts'].min()
            end = df['ts'].max()
            trades_count = len(df)
            report.append({
                'Symbol': s,
                'Start (UTC)': start,
                'End (UTC)': end,
                'Duration': end - start,
                'Ticks Analyzed': trades_count
            })
    
    df_rep = pd.DataFrame(report)
    print("\n--- DETAILED STRESS TEST LOG (Rank 100-1500) ---")
    print(df_rep.to_string(index=False))

if __name__ == "__main__":
    get_detailed_log()
