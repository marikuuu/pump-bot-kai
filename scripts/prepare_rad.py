import asyncio
import os
import sys

# Add current directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.fetch_history import HistoryFetcher

async def prepare():
    symbol = 'RAD/USDT:USDT'
    exch = 'binance'
    fetcher = HistoryFetcher(exch)
    
    print(f"--- PREPARING DATA FOR {symbol} ({exch}) ---")
    await fetcher.fetch_ohlcv(symbol, days=2)
    # Fetch trades but force small window to avoid timeout
    await fetcher.fetch_trades(symbol, days=0.5) 
    
    # Verify files on disk specifically
    data_dir = os.path.join(os.getcwd(), 'data', 'history')
    print(f"Directory: {data_dir}")
    print("Files found:")
    for f in os.listdir(data_dir):
        if 'RAD' in f:
            print(f" - {f} ({os.path.getsize(os.path.join(data_dir, f))} bytes)")
            
    await fetcher.close()

if __name__ == "__main__":
    asyncio.run(prepare())
