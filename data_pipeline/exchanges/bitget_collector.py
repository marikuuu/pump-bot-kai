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

class BitgetCollector:
    """
    Collects real-time Bitget trade data for Cross-Exchange validation.
    """
    def __init__(self, symbols: List[str] = None, db_manager: DatabaseManager = None):
        self.exchange_id = 'bitget'
        self.symbols = symbols or []
        self.exchange = ccxtpro.bitget({
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'} # Bitget Futures
        })
        self.db = db_manager or DatabaseManager()
        self.detector = None # Will be injected from main.py
        
        self.trade_buffers: Dict[str, List[Dict]] = {}
        self.history: Dict[str, pd.DataFrame] = {}

    async def initialize(self):
        if not self.db.pool:
            await self.db.connect()
        # Specific targets requested by user
        targets = ["SIREN/USDT:USDT", "TRIA/USDT:USDT", "JCT/USDT:USDT", "LYN/USDT:USDT", "LIGHT/USDT:USDT"]
        self.symbols = [s for s in targets]
        
        for s in self.symbols:
            self.trade_buffers[s] = []
            self.history[s] = pd.DataFrame()
            
        logging.info(f"BitgetCollector initialized with {len(self.symbols)} strategy symbols.")

    async def watch_trades(self, symbol: str):
        logging.info(f"Starting Bitget trade watcher for {symbol}")
        while True:
            try:
                trades = await self.exchange.watch_trades(symbol)
                for trade in trades:
                    trade['received_at'] = time.time()
                    self.trade_buffers[symbol].append(trade)
            except Exception as e:
                logging.error(f"Error in Bitget {symbol} loop: {e}")
                await asyncio.sleep(5)

    async def scheduler_loop(self):
        while True:
            await asyncio.sleep(30)
            for symbol in self.symbols:
                await self.process_chunk(symbol)

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

        if self.detector and self.detector.check_event(features):
            logging.warning(f"🚀 BITGET PUMP SIGNAL: {symbol} | vol_z={vol_z:.2f} pc_z={pc_z:.2f}")

    async def run(self):
        await self.initialize()
        watchers = [self.watch_trades(s) for s in self.symbols]
        watchers.append(self.scheduler_loop())
        await asyncio.gather(*watchers)

if __name__ == "__main__":
    c = BitgetCollector()
    asyncio.run(c.run())
