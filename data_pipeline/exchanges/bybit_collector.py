import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import ccxt.pro as ccxtpro
import pandas as pd
import numpy as np
from dotenv import load_dotenv

from database.db_manager import DatabaseManager
from pump_ai.detector import PumpDetector

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BybitCollector:
    """
    Collects real-time Bybit trade data for Cross-Exchange validation.
    """
    def __init__(self, symbols: List[str] = None, db_manager: DatabaseManager = None):
        self.exchange_id = 'bybit'
        self.symbols = symbols or []
        self.exchange = ccxtpro.bybit({
            'enableRateLimit': True,
            'options': {'defaultType': 'linear'} # Bybit Futures
        })
        self.db = db_manager or DatabaseManager()
        self.detector = None # Will be injected from main.py
        
        self.trade_buffers: Dict[str, List[Dict]] = {}
        self.history: Dict[str, pd.DataFrame] = {}

    async def initialize(self):
        if not self.db.pool:
            await self.db.connect()
        
        # Auto-Discovery for Bybit Gems
        if os.getenv("CEX_SYMBOLS") == "AUTO":
            try:
                markets = await self.exchange.load_markets()
                candidates = []
                for sym, m in markets.items():
                    if m.get('active') and m.get('linear') and '/USDT:USDT' in sym:
                         # Bybit volume is often in 'info'
                         qv = float(m.get('info', {}).get('turnover24h', 0)) or 0
                         if 500_000 < qv < 50_000_000:
                             candidates.append((sym, qv))
                
                candidates.sort(key=lambda x: x[1], reverse=True)
                self.symbols = [s for s, _ in candidates[:50]]
                logging.info(f"Bybit Discovery Success: {len(self.symbols)} symbols found. e.g. {self.symbols[:5]}")
            except Exception as e:
                logging.error(f"Bybit Discovery failed: {e}")
                self.symbols = ["BTC/USDT:USDT"]
        
        for s in self.symbols:
            self.trade_buffers[s] = []
            self.history[s] = pd.DataFrame()
            
        logging.info(f"BybitCollector initialized with {len(self.symbols)} symbols.")

    async def watch_trades(self, symbol: str):
        logging.info(f"Starting Bybit trade watcher for {symbol}")
        while True:
            try:
                trades = await self.exchange.watch_trades(symbol)
                for trade in trades:
                    trade['received_at'] = time.time()
                    self.trade_buffers[symbol].append(trade)
            except Exception as e:
                logging.error(f"Error in Bybit {symbol} loop: {e}")
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
