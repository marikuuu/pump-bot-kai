import ccxt
import pandas as pd
from datetime import datetime, timezone, timedelta
import time

def find_all_pumps():
    # Using Sync CCXT
    exchange = ccxt.binance({'options': {'defaultType': 'swap'}})
    # Period: 3/16 - 3/21
    since = int(datetime(2026, 3, 16, tzinfo=timezone.utc).timestamp() * 1000)
    
    markets = exchange.load_markets()
    symbols = [s for s in markets if (s.endswith('/USDT:USDT') or s.endswith('/USDT'))]
    
    print(f"Checking {len(symbols)} symbols for 1.5x pumps since 3/16...")
    
    pumps = []
    # Limit symbols for speed if too many, but let's try all
    for s in symbols[:200]: # Check first 200 for now to avoid long wait
        try:
            # 4h candles are enough to find 1.5x moves in a week
            candles = exchange.fetch_ohlcv(s, timeframe='4h', since=since, limit=100)
            if not candles: continue
            
            prices = [c[4] for c in candles]
            min_p = min(prices)
            max_p = max(prices)
            
            gain = (max_p - min_p) / min_p
            if gain >= 0.5: # 1.5x
                pumps.append({'symbol': s, 'max_gain': gain})
                print(f"FOUND: {s} | Max Gain: {gain*100:.1f}%")
            time.sleep(0.05)
        except Exception:
            pass
            
    print("\n=== Real Pumps Found (3/16-3/21) ===")
    for p in pumps:
        print(f"{p['symbol']}: {p['max_gain']*100:.1f}%")

if __name__ == "__main__":
    find_all_pumps()
