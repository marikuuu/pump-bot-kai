import asyncio
import ccxt.async_support as ccxt
import pandas as pd
import os
import json
import logging
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class MultiverseTickFetcher:
    def __init__(self):
        self.exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'}
        })
        self.data_dir = 'data/history/multiverse/ticks'
        os.makedirs(self.data_dir, exist_ok=True)
        self.semaphore = asyncio.Semaphore(5) # Conservative to avoid bans

    async def fetch_event_ticks(self, event):
        async with self.semaphore:
            symbol = event['symbol']
            start_ts = event['start_ts']
            end_ts = event['end_ts']
            
            clean = symbol.replace('/', '_').replace(':', '_')
            filename = os.path.join(self.data_dir, f"{clean}_{start_ts}_ticks.csv")
            
            if os.path.exists(filename): return
            
            try:
                # Fetch trades for the 30m window
                # Fetch in chunks of 1000
                all_trades = []
                since = start_ts
                while since < end_ts:
                    trades = await self.exchange.fetch_trades(symbol, since=since, limit=1000)
                    if not trades: break
                    all_trades.extend(trades)
                    since = trades[-1]['timestamp'] + 1
                    if len(trades) < 1000: break
                    if len(all_trades) > 5000: break # Cap for speed
                
                if all_trades:
                    df = pd.DataFrame(all_trades)
                    df.to_csv(filename, index=False)
                    # logging.info(f"FETCHED {len(df)} ticks for {symbol} at {start_ts}")
                
            except Exception as e:
                logging.error(f"ERROR {symbol} ({start_ts}): {e}")

    async def run(self, candidates_file='multiverse_candidates.json', priority_file='priority_pumps.json'):
        targets = []
        if os.path.exists(priority_file):
            with open(priority_file, 'r') as f:
                pumps = json.load(f)
                # Ensure they have end_ts (30m window for features)
                for p in pumps:
                    p['start_ts'] = p['ts'] - (15 * 60 * 1000)
                    p['end_ts'] = p['ts'] + (15 * 60 * 1000)
                    # Correct normalization: AERO/USDT -> AERO/USDT:USDT
                    if ':' not in p['symbol']:
                        s = p['symbol']
                        if '/' not in s:
                            s = s.replace('USDT', '/USDT')
                        p['symbol'] = f"{s}:USDT"
                targets.extend(pumps)
            print(f"Loaded {len(pumps)} priority pumps.")
            
        if os.path.exists(candidates_file):
            with open(candidates_file, 'r') as f:
                candidates = json.load(f)
            # Sort by Z-score and add top 1000 as noise
            candidates.sort(key=lambda x: x['peak_vol_z'], reverse=True)
            targets.extend(candidates[:1000])
            
        print(f"Total fetching targets: {len(targets)}")
        
        tasks = [self.fetch_event_ticks(t) for t in targets]
        await asyncio.gather(*tasks)
        await self.exchange.close()

if __name__ == "__main__":
    fetcher = MultiverseTickFetcher()
    asyncio.run(fetcher.run())
