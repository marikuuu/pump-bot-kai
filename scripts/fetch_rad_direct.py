import asyncio
import ccxt.async_support as ccxt
import pandas as pd
from datetime import datetime, timedelta, timezone
import os

async def fetch_rad_direct():
    b = ccxt.binance({'options': {'defaultType': 'swap'}})
    symbol = 'RAD/USDT:USDT'
    # Event: 2026-03-22 04:12:00
    target = datetime.fromisoformat('2026-03-22T04:12:00').replace(tzinfo=timezone.utc)
    # Start 30 mins before
    since = int((target - timedelta(minutes=60)).timestamp() * 1000)
    
    print(f"Fetching RAD trades from {target - timedelta(minutes=60)}...")
    all_trades = []
    
    try:
        for _ in range(30): # Fetch up to 30k trades (should cover the window)
            trades = await b.fetch_trades(symbol, since)
            if not trades: break
            all_trades.extend(trades)
            since = trades[-1]['timestamp'] + 1
            if since > int((target + timedelta(minutes=30)).timestamp() * 1000): break
            await asyncio.sleep(0.5)
            
        if all_trades:
            df = pd.DataFrame(all_trades)
            path = 'data/history/RAD_USDT_USDT_trades_binance.csv'
            # Ensure directory
            os.makedirs('data/history', exist_ok=True)
            df.to_csv(path, index=False)
            print(f"SAVED {len(all_trades)} trades to {path}")
        else:
            print("FAILED TO FETCH TRADES.")
            
    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        await b.close()

if __name__ == "__main__":
    asyncio.run(fetch_rad_direct())
