import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import numpy as np
import websockets
from dotenv import load_dotenv

from database.db_manager import DatabaseManager
from pump_ai.detector import PumpDetector

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BitgetCollector:
    """
    Collects real-time Bitget trade data using Direct API (No CCXT).
    """
    def __init__(self, symbols: List[str] = None, db_manager: DatabaseManager = None):
        self.exchange_id = 'bitget'
        self.symbols = symbols or []
        self.db = db_manager or DatabaseManager()
        self.detector = None # Will be injected from main.py
        
        self.trade_buffers: Dict[str, List[Dict]] = {}
        self.history: Dict[str, pd.DataFrame] = {}

    async def initialize(self):
        import aiohttp
        if not self.db.pool:
            await self.db.connect()
        
        # Native Bitget V2 Discovery
        if os.getenv("CEX_SYMBOLS") == "AUTO":
            logging.info("Native Bitget Discovery: hitting api.bitget.com directly...")
            try:
                url = "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        data = await resp.json()
                        tickers = data.get('data', [])
                        candidates = []
                        
                        stocks_to_exclude = ['NVDA', 'AAPL', 'TSLA', 'MSTR', 'MU', 'AMZN', 'GOOG', 'META', 'NFLX', 'MSFT', 'COIN', 'HOOD']
                        
                        for t in tickers:
                            sym = t.get('symbol', '') # Bitget Native: BTCUSDT
                            if not sym.endswith('USDT'): continue
                            
                            base = sym.replace('USDT', '')
                            if base in stocks_to_exclude: continue
                            
                            qv = float(t.get('quoteVolume') or 0)
                            if 500_000 < qv < 20_000_000:
                                # Internal format: BASE/USDT
                                candidates.append((f"{base}/USDT", qv))
                        
                        candidates.sort(key=lambda x: x[1], reverse=True)
                        self.symbols = [s for s, _ in candidates[:50]]
                        logging.info(f"Native Bitget Discovery: {len(self.symbols)} symbols (Unified Format) found.")
            except Exception as e:
                logging.error(f"Native Bitget Discovery failed: {e}")
                self.symbols = ["BTCUSDT"]
        
        for s in self.symbols:
            self.trade_buffers[s] = []
            self.history[s] = pd.DataFrame()
            
        logging.info(f"BitgetCollector initialized with {len(self.symbols)} symbols.")

    async def watch_trades(self, symbol: str):
        import json
        import websockets
        # Internal: BASE/USDT -> Bitget Native: BASEUSDT
        clean_sym = symbol.replace('/', '').upper()
        ws_url = "wss://ws.bitget.com/v2/ws/public"
        
        logging.info(f"Starting Native Bitget WS for {symbol}")
        while True:
            try:
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                    # Subscribe
                    sub_msg = json.dumps({
                        "op": "subscribe",
                        "args": [{
                            "instType": "USDT-FUTURES",
                            "channel": "trade",
                            "instId": clean_sym
                        }]
                    })
                    await ws.send(sub_msg)
                    
                    while True:
                        msg = await ws.recv()
                        res = json.loads(msg)
                        if 'data' not in res: continue
                        
                        for d in res['data']:
                            trade = {
                                'price': float(d['price']),
                                'amount': float(d['size']),
                                'side': d['side'].lower(),
                                'timestamp': int(d['ts']),
                                'received_at': time.time()
                            }
                            self.trade_buffers[symbol].append(trade)
            except Exception as e:
                logging.error(f"Native Bitget WS Error ({symbol}): {e}")
                await asyncio.sleep(5)

    async def scheduler_loop(self):
        while True:
            await asyncio.sleep(30)
            for symbol in self.symbols:
                try:
                    await self.process_chunk(symbol)
                except Exception as e:
                    logging.error(f"Error processing {symbol} on Bitget: {e}")

    async def process_chunk(self, symbol: str):
        trades = self.trade_buffers[symbol]
        self.trade_buffers[symbol] = []
        if not trades: return

        df = pd.DataFrame(trades)
        price_end = df['price'].iloc[-1]
        vol = df['amount'].sum()
        
        df['ts_dt'] = pd.to_datetime(df['received_at'], unit='s')
        df['sec'] = df['ts_dt'].dt.second
        buy_df = df[df['side'] == 'buy']
        rush_counts = buy_df.groupby('sec').size().reindex(range(0, 60), fill_value=0)
        std_rush = float(rush_counts.std())

        price_change = (price_end - df['price'].iloc[0]) / df['price'].iloc[0]
        new_row = {'volume': vol, 'price_change': price_change}
        self.history[symbol] = pd.concat([self.history[symbol], pd.DataFrame([new_row])]).iloc[-120:]

        if len(self.history[symbol]) < 10: return

        vol_z = (vol - self.history[symbol]['volume'].mean()) / (self.history[symbol]['volume'].std() or 1)
        pc_z = (price_change - self.history[symbol]['price_change'].mean()) / (self.history[symbol]['price_change'].std() or 1)

        features = {
            'symbol': symbol,
            'price': price_end,
            'vol_z': vol_z,
            'pc_z': pc_z,
            'std_rush': std_rush,
            'buy_ratio': len(buy_df) / (len(df) + 1e-9),
            'exchange': 'BITGET'
        }

        if self.detector and self.detector.check_event(features)[0]:
            logging.warning(f"🚀 BITGET PUMP SIGNAL: {symbol} | vol_z={vol_z:.2f} pc_z={pc_z:.2f}")

    async def run(self):
        await self.initialize()
        
        # Staggered start to avoid rate limits (code 30006)
        watchers = []
        for s in self.symbols:
            watchers.append(asyncio.create_task(self.watch_trades(s)))
            await asyncio.sleep(1.0) # 1.0s delay between connections
            
        watchers.append(asyncio.create_task(self.scheduler_loop()))
        await asyncio.gather(*watchers)

if __name__ == "__main__":
    c = BitgetCollector()
    asyncio.run(c.run())
