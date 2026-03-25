import ccxt
import pandas as pd
from datetime import datetime, timezone, timedelta
import asyncio

async def find_all_pumps():
    exchange = ccxt.binance({'options': {'defaultType': 'swap'}})
    # Period: 3/16 - 3/21
    since = int(datetime(2026, 3, 16, tzinfo=timezone.utc).timestamp() * 1000)
    end = int(datetime(2026, 3, 21, tzinfo=timezone.utc).timestamp() * 1000)
    
    markets = await exchange.load_markets()
    symbols = [s for s in markets if s.endswith('/USDT:USDT')]
    
    print(f"Checking {len(symbols)} symbols for 1.5x pumps...")
    
    pumps = []
    for s in symbols:
        try:
            # Fetch daily or 4h candles to find max gain
            candles = await exchange.fetch_ohlcv(s, timeframe='1h', since=since, limit=1000)
            if not candles: continue
            
            # Find min price and max price in the window
            prices = [c[4] for c in candles] # close
            min_p = min(prices)
            max_p = max(prices)
            
            gain = (max_p - min_p) / min_p
            if gain >= 0.5: # 1.5x
                pumps.append({'symbol': s, 'max_gain': gain})
                print(f"FOUND: {s} | Gain: {gain*100:.1f}%")
            await asyncio.sleep(0.05)
        except Exception:
            pass
            
    print("\n=== All 1.5x Pumps Found (3/16-3/21) ===")
    for p in pumps:
        print(f"{p['symbol']}: {p['max_gain']*100:.1f}%")
        
    await exchange.close()

if __name__ == "__main__":
    asyncio.run(find_all_pumps())
