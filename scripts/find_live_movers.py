import asyncio
import ccxt.async_support as ccxt
import pandas as pd
from datetime import datetime, timedelta, timezone

async def find_live_movers():
    b = ccxt.binance({'options': {'defaultType': 'swap'}})
    print("Fetching Top Movers (1h) on Binance...")
    
    markets = await b.load_markets()
    tickers = await b.fetch_tickers()
    
    movers = []
    for s, t in tickers.items():
        if '/USDT:USDT' in s and t['percentage'] is not None:
            movers.append({'symbol': s, 'change': t['percentage']})
            
    # Sort by absolute change (volatility)
    movers.sort(key=lambda x: abs(x['change']), reverse=True)
    
    print("\nTop 5 Most Volatile Symbols:")
    for m in movers[:5]:
        print(f" - {m['symbol']}: {m['change']:.2%}")
        
    # Pick the top one for the proof
    top_symbol = movers[0]['symbol']
    print(f"\nProceeding with {top_symbol}...")
    
    # Store for next script
    with open("live_symbol.txt", "w") as f:
        f.write(top_symbol)
        
    await b.close()

if __name__ == "__main__":
    asyncio.run(find_live_movers())
