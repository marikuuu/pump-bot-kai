import asyncio
import ccxt.async_support as ccxt
import pandas as pd
from datetime import datetime, timedelta, timezone

async def find_recent_explosion():
    b = ccxt.binance({'options': {'defaultType': 'swap'}})
    m = ccxt.mexc({'options': {'defaultType': 'swap'}})
    
    # Check Top 50 by volume (more likely to have "explosions")
    for exch in [b, m]:
        print(f"Scanning {exch.id}...")
        markets = await exch.load_markets()
        # Sort by volume
        symbols = [s for s in markets if '/USDT' in s and 'options' not in markets[s]]
        
        for s in symbols[:40]:
            try:
                # 1m candles for last 4 hours
                now = int(datetime.now(timezone.utc).timestamp() * 1000)
                since = now - (4 * 60 * 60 * 1000)
                ohlcv = await exch.fetch_ohlcv(s, '1m', since=since, limit=240)
                if not ohlcv: continue
                df = pd.DataFrame(ohlcv, columns=['t','o','h','l','c','v'])
                
                # Check for 4% move in 10 mins
                df['pump'] = (df['h'].rolling(10).max().shift(-10) - df['c']) / df['c']
                
                if df['pump'].max() > 0.04:
                    row = df.loc[df['pump'].idxmax()]
                    event_time = datetime.fromtimestamp(row['t']/1000, tz=timezone.utc)
                    print(f"!!! [RECENT EXPLOSION FOUND] {s} on {exch.id} !!!")
                    print(f"Time: {event_time}")
                    print(f"Gain: {row['pump']:.2%}")
                    
                    # Store for next step
                    with open("recent_event.txt", "w") as f:
                        f.write(f"{exch.id},{s},{event_time.isoformat()}")
                    
                    await exch.close()
                    return
            except: continue
            
    await b.close()
    await m.close()

if __name__ == "__main__":
    asyncio.run(find_recent_explosion())
