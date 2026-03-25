import asyncio
import logging
import os
import time
import json
import websockets
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import numpy as np
from dotenv import load_dotenv

from database.db_manager import DatabaseManager
from pump_ai.detector import PumpDetector

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BybitCollector:
    """
    Collects real-time Bybit trade data using Direct V5 API (No CCXT).
    """
    def __init__(self, symbols: List[str] = None, db_manager: DatabaseManager = None):
        self.exchange_id = 'bybit'
        self.symbols = symbols or []
        self.db = db_manager or DatabaseManager()
        self.detector = None # Will be injected from main.py
        
        self.trade_buffers: Dict[str, List[Dict]] = {}
        self.history: Dict[str, pd.DataFrame] = {}

    async def initialize(self):
        import aiohttp
        if not self.db.pool:
            await self.db.connect()
        
        # Native Bybit V5 Discovery
        try:
            logging.info("Native Bybit V5 Discovery: hitting instruments-info directly...")
            url = "https://api.bybit.com/v5/market/instruments-info?category=linear"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    data = await resp.json()
                    res = data.get('result', {}).get('list', [])
                    candidates = []
                    for item in res:
                        if item.get('status') == 'Trading':
                            base = item.get('baseCoin')
                            unified_sym = f"{base}/USDT"
                            candidates.append(unified_sym)
                    
                    self.symbols = candidates[:50]
                    logging.info(f"Native Bybit Discovery Success: {len(self.symbols)} symbols (Unified Format).")
        except Exception as e:
            logging.error(f"Native Bybit Discovery FAILED: {e}")

        if not self.symbols:
             logging.warning("Bybit Discovery returned 0 symbols. Bridge might be needed.")
        
        for s in self.symbols:
            self.trade_buffers[s] = []
            self.history[s] = pd.DataFrame()
            
        logging.info(f"BybitCollector initialized with {len(self.symbols)} symbols.")

    async def watch_trades(self, symbol: str):
        import json
        import websockets
        # Internal: BASE/USDT -> Bybit Native: BASEUSDT
        clean_sym = symbol.replace('/', '').upper()
        ws_url = "wss://stream.bybit.com/v5/public/linear"
        
        logging.info(f"Starting Native Bybit WS for {symbol}")
        while True:
            try:
                async with websockets.connect(ws_url) as ws:
                    # Subscribe
                    sub_msg = json.dumps({"op": "subscribe", "args": [f"publicTrade.{clean_sym}"]})
                    await ws.send(sub_msg)
                    
                    while True:
                        msg = await ws.recv()
                        res = json.loads(msg)
                        if 'data' not in res: continue
                        
                        for d in res['data']:
                            trade = {
                                'price': float(d['p']),
                                'amount': float(d['v']),
                                'side': d['S'].lower(), # Buy/Sell
                                'timestamp': int(d['T']),
                                'received_at': time.time()
                            }
                            self.trade_buffers[symbol].append(trade)
            except Exception as e:
                logging.error(f"Native Bybit WS Error ({symbol}): {e}")
                await asyncio.sleep(5)

    async def scheduler_loop(self):
        while True:
            await asyncio.sleep(30)
            for symbol in self.symbols:
                try:
                    await self.process_chunk(symbol)
                except Exception as e:
                    logging.error(f"Error processing {symbol} on Bybit: {e}")

    async def process_chunk(self, symbol: str):
        trades = self.trade_buffers[symbol]
        self.trade_buffers[symbol] = []
        if not trades: return

        df = pd.DataFrame(trades)
        price_end = df['price'].iloc[-1]
        vol = df['amount'].sum()
        
        # Calculate v5.1 feature set (Simplified for Cross-Ex validation)
        df['ts_dt'] = pd.to_datetime(df['received_at'], unit='s')
        df['sec'] = df['ts_dt'].dt.second
        buy_df = df[df['side'] == 'buy']
        rush_counts = buy_df.groupby('sec').size().reindex(range(0, 60), fill_value=0)
        std_rush = float(rush_counts.std())

        price_change = (price_end - df['price'].iloc[0]) / (df['price'].iloc[0] + 1e-9)
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
            'exchange': 'BYBIT'
        }

        if self.detector and self.detector.check_event(features)[0]:
            logging.warning(f"🚀 BYBIT PUMP SIGNAL: {symbol} | vol_z={vol_z:.2f} pc_z={pc_z:.2f}")

    async def run(self):
        await self.initialize()
        
        watchers = []
        for s in self.symbols:
            watchers.append(asyncio.create_task(self.watch_trades(s)))
            await asyncio.sleep(1.0)
            
        watchers.append(asyncio.create_task(self.scheduler_loop()))
        await asyncio.gather(*watchers)

if __name__ == "__main__":
    c = BybitCollector()
    asyncio.run(c.run())
