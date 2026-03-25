import asyncio
import ccxt.async_support as ccxt
import pandas as pd
import os
import logging
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class MultiverseFetcher:
    def __init__(self):
        self.exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'}
        })
        self.data_dir = 'data/history/multiverse'
        os.makedirs(self.data_dir, exist_ok=True)
        self.semaphore = asyncio.Semaphore(10) # 10 parallel requests

    async def fetch_symbol_ohlcv(self, symbol, days=7):
        async with self.semaphore:
            filename = os.path.join(self.data_dir, f"{symbol.replace('/', '_').replace(':', '_')}_1m.csv")
            if os.path.exists(filename):
                # logging.info(f"Skipping {symbol}, already exists.")
                return
            
            try:
                now = datetime.now(timezone.utc)
                since = int((now - timedelta(days=days)).timestamp() * 1000)
                all_ohlcv = []
                
                # Fetch in chunks of 1000 (Binance limit)
                while since < int(now.timestamp() * 1000):
                    ohlcv = await self.exchange.fetch_ohlcv(symbol, '1m', since=since, limit=1000)
                    if not ohlcv: break
                    all_ohlcv.extend(ohlcv)
                    since = ohlcv[-1][0] + 1
                    if len(ohlcv) < 1000: break
                
                if all_ohlcv:
                    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df.to_csv(filename, index=False)
                    logging.info(f"DONE: {symbol} ({len(df)} rows)")
                else:
                    logging.warning(f"EMPTY: {symbol}")
            except Exception as e:
                logging.error(f"ERROR {symbol}: {e}")

    async def run(self, max_symbols=500):
        await self.exchange.load_markets()
        symbols = [s for s in self.exchange.symbols if '/USDT:USDT' in s]
        # Skip top 30 to focus on "Grass Coins"
        targets = symbols[30:30+max_symbols]
        print(f"Targeting {len(targets)} symbols for 7 days.")
        
        tasks = [self.fetch_symbol_ohlcv(s) for s in targets]
        await asyncio.gather(*tasks)
        await self.exchange.close()

if __name__ == "__main__":
    fetcher = MultiverseFetcher()
    asyncio.run(fetcher.run(max_symbols=500))
