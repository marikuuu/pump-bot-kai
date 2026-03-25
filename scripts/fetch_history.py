import asyncio
import os
import logging
import pandas as pd
from datetime import datetime, timedelta, timezone
import ccxt.async_support as ccxt
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class HistoryFetcher:
    """
    Robust downloader for historical OHLCV, Trade, and Open Interest data.
    """
    def __init__(self, exchange_id='binance'):
        self.exchange_id = exchange_id
        self.exchange = getattr(ccxt, exchange_id)({
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'}
        })
        self.data_dir = 'data/history'
        os.makedirs(self.data_dir, exist_ok=True)

    def get_filenames(self, symbol: str, timeframe='1m'):
        clean = symbol.replace('/', '_').replace(':', '_')
        return {
            'ohlcv': f"{self.data_dir}/{clean}_{timeframe}_{self.exchange_id}.csv",
            'trades': f"{self.data_dir}/{clean}_trades_{self.exchange_id}.csv",
            'oi': f"{self.data_dir}/{clean}_oi_{self.exchange_id}.csv"
        }

    async def fetch_ohlcv(self, symbol: str, timeframe='1m', days=30):
        logging.info(f"--- Fetching OHLCV: {symbol} | {timeframe} | {days} days ---")
        now = datetime.now(timezone.utc)
        since = int((now - timedelta(days=days)).timestamp() * 1000)
        all_ohlcv = []
        
        while since < int(now.timestamp() * 1000):
            try:
                ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, since)
                if not ohlcv: break
                since = ohlcv[-1][0] + 1
                all_ohlcv.extend(ohlcv)
                logging.info(f"Fetched {len(all_ohlcv)} OHLCV rows...")
                await asyncio.sleep(self.exchange.rateLimit / 1000)
            except Exception as e:
                logging.error(f"Error fetching OHLCV: {e}")
                break
        
        if all_ohlcv:
            df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['time'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            df.to_csv(self.get_filenames(symbol, timeframe)['ohlcv'], index=False)
            return df
        return None

    async def fetch_trades(self, symbol: str, days=1):
        logging.info(f"--- Fetching Trades: {symbol} | {days} days ---")
        now = datetime.now(timezone.utc)
        since = int((now - timedelta(days=days)).timestamp() * 1000)
        all_trades = []
        
        while since < int(now.timestamp() * 1000):
            try:
                trades = await self.exchange.fetch_trades(symbol, since)
                if not trades: break
                since = trades[-1]['timestamp'] + 1
                all_trades.extend(trades)
                logging.info(f"Fetched {len(all_trades)} trades...")
                if len(all_trades) > 100000: break # Safety cap
                await asyncio.sleep(self.exchange.rateLimit / 1000)
            except Exception as e:
                logging.error(f"Error fetching trades: {e}")
                break
                
        if all_trades:
            df = pd.DataFrame(all_trades)
            df.to_csv(self.get_filenames(symbol)['trades'], index=False)
            return df
        return None

    async def fetch_oi_history(self, symbol: str, timeframe='5m', days=7):
        """
        Fetches historical Open Interest data. (Supported on Binance/Bybit/Bitget).
        """
        logging.info(f"--- Fetching OI History: {symbol} | {days} days ---")
        if not hasattr(self.exchange, 'fetch_open_interest_history'):
            logging.warning(f"{self.exchange_id} does not support fetch_open_interest_history")
            return None
            
        now = datetime.now(timezone.utc)
        since = int((now - timedelta(days=days)).timestamp() * 1000)
        all_oi = []
        
        while since < int(now.timestamp() * 1000):
            try:
                # 'period' is typical for Binance OI history (5m, 1h, etc)
                oi_list = await self.exchange.fetch_open_interest_history(symbol, timeframe, since)
                if not oi_list: break
                since = oi_list[-1]['timestamp'] + 1
                all_oi.extend(oi_list)
                logging.info(f"Fetched {len(all_oi)} OI records...")
                await asyncio.sleep(self.exchange.rateLimit / 1000)
            except Exception as e:
                logging.error(f"Error fetching OI history: {e}")
                break
                
        if all_oi:
            df = pd.DataFrame(all_oi)
            df['time'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            df.to_csv(self.get_filenames(symbol)['oi'], index=False)
            logging.info(f"Saved OI history to {self.get_filenames(symbol)['oi']}")
            return df
        return None

    async def discover_low_cap_futures(self, limit=20):
        """
        Targets assets likely in the Rank 100-1500 range (Low volume, but traded).
        """
        logging.info("Searching for Rank 100-1500 style futures (Low Volume Assets)...")
        await self.exchange.load_markets()
        
        futures = [s for s, m in self.exchange.markets.items() if m.get('swap') and m.get('linear') and 'USDT' in s]
        tickers = await self.exchange.fetch_tickers(futures)
        
        # Filter: Exclude top volume assets (BTC, ETH, SOL, etc)
        # Sort by Quote Volume (USDT Volume)
        sorted_tickers = sorted(
            [t for t in tickers.values() if t.get('quoteVolume')],
            key=lambda x: x['quoteVolume']
        )
        
        # Take assets in the 10th-50th percentile of volume (likely mid-to-small cap)
        # Avoid extremely dead coins (vol < 50k)
        candidates = [t['symbol'] for t in sorted_tickers if t['quoteVolume'] > 50000]
        
        # Rank 100-1500 on Binance are usually in the lower 1/3 of the futures list by volume
        targets = candidates[:limit]
        logging.info(f"Identified small-cap targets: {targets}")
        return targets

    async def close(self):
        await self.exchange.close()

if __name__ == "__main__":
    async def run():
        fetcher = HistoryFetcher('binance')
        s = 'BTC/USDT:USDT'
        await fetcher.fetch_ohlcv(s, days=1)
        await fetcher.fetch_trades(s, days=0.1)
        await fetcher.fetch_oi_history(s, days=1)
        await fetcher.close()
    asyncio.run(run())
