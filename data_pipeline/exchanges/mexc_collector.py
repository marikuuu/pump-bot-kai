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
from database.data_logger import DataLogger

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MexcCollector:
    """
    Collects real-time MEXC trade data using Direct API (No CCXT).
    """
    def __init__(self, symbols: List[str] = None, db_manager: DatabaseManager = None):
        self.exchange_id = 'mexc'
        self.symbols = symbols or []
        self.db = db_manager or DatabaseManager()
        self.logger = DataLogger(self.db)
        self.detector = PumpDetector()
        
        self.trade_buffers: Dict[str, List[Dict]] = {}
        self.last_chunk_time: Dict[str, float] = {}
        self.history: Dict[str, pd.DataFrame] = {}
        self.market_caps: Dict[str, float] = {}

    async def initialize(self):
        import aiohttp
        if not self.db.pool:
            await self.db.connect()
        
        # Native MEXC Discovery (Bypassing CCXT for contract stability)
        if not self.symbols or self.symbols == ["AUTO"]:
            try:
                logging.info("Native MEXC Discovery: hitting contract.mexc.com directly...")
                url = "https://contract.mexc.com/api/v1/contract/detail"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        data = await resp.json()
                        contracts = data.get('data', [])
                        candidates = []
                        for c in contracts:
                            raw_sym = c.get('symbol', '')
                            if c.get('state') == 0 and raw_sym.endswith('_USDT'):
                                # Map BTC_USDT -> BTC/USDT (Internal Unified)
                                base = raw_sym.replace('_USDT', '')
                                unified_sym = f"{base}/USDT"
                                candidates.append(unified_sym)
                        
                        self.symbols = candidates[:30]
                        logging.info(f"Native MEXC Discovery Success: {len(self.symbols)} symbols (Unified Format).")
            except Exception as e:
                logging.error(f"Native MEXC Discovery FAILED: {e}")

        if not self.symbols or self.symbols == ["AUTO"]:
            try:
                # Force MEXC Contract API (swap)
                markets = await self.exchange.fetch_markets(params={'type': 'swap'})
                candidates = []
                
                # Handle both list and dict returns from CCXT
                market_items = markets.values() if isinstance(markets, dict) else markets
                
                for m in market_items:
                    sym = m.get('symbol', '')
                    if m.get('active') and '_' in sym: # Native MEXC style
                        qv = float(m.get('info', {}).get('quoteVolume', 0)) or 0
                        if 100_000 < qv:
                             candidates.append((sym, qv))
                             self.market_caps[sym] = qv
                
                candidates.sort(key=lambda x: x[1], reverse=True)
                self.symbols = [s for s, _ in candidates[:30]]
                
                # Standard core targets (Native format)
                u_targets = ["SIREN_USDT", "TRIA_USDT", "JCT_USDT", "LYN_USDT", "LIGHT_USDT"]
                for ut in u_targets:
                    if ut not in self.symbols: self.symbols.append(ut)
                
                logging.info(f"MEXC Discovery Success: {len(self.symbols)} symbols.")
            except Exception as e:
                logging.error(f"MEXC Discovery FAILED: {e}")
                self.symbols = ["SIREN_USDT", "TRIA_USDT", "JCT_USDT", "LYN_USDT"]
        
        for s in self.symbols:
            self.trade_buffers[s] = []
            self.last_chunk_time[s] = time.time()
            self.history[s] = pd.DataFrame()
            if s not in self.market_caps: self.market_caps[s] = 1_000_000
            
        logging.info(f"MexcCollector initialized ({len(self.symbols)} symbols).")

    async def watch_combined_trades(self):
        """
        Watches all symbols using a single shared WebSocket connection.
        """
        import json
        import websockets
        ws_url = "wss://contract.mexc.com/edge"
        
        logging.info(f"Starting Combined MEXC WS for {len(self.symbols)} symbols")
        while True:
            try:
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                    sym_map = {} # BTC_USDT -> BTC/USDT or BTC_USDT (if no slash)
                    # Subscribe to all symbols
                    for symbol in self.symbols:
                        native_sym = symbol.replace('/', '_') if '/' in symbol else symbol
                        sym_map[native_sym] = symbol
                        sub_msg = json.dumps({"method": "sub.deal", "param": {"symbol": native_sym}})
                        await ws.send(sub_msg)
                        await asyncio.sleep(0.05) # Tiny stagger for subscriptions
                    
                    while True:
                        msg = await ws.recv()
                        res = json.loads(msg)
                        
                        # Bulletproof handling for MEXC WS data structure quirks
                        def flatten(node):
                            if isinstance(node, list):
                                for n in node: yield from flatten(n)
                            elif isinstance(node, dict):
                                yield node

                        for item in flatten(res):
                            if not isinstance(item, dict) or item.get('channel') != 'push.deal': continue
                            
                            native_sym = item.get('symbol')
                            unified_sym = sym_map.get(native_sym, native_sym)
                            
                            data_node = item.get('data', {})
                            trades_to_process = data_node if isinstance(data_node, list) else [data_node]
                            
                            for d in trades_to_process:
                                if not isinstance(d, dict): continue
                                try:
                                    trade = {
                                        'price': float(d.get('p', 0)),
                                        'amount': float(d.get('v', 0)),
                                        'side': 'buy' if d.get('T') == 1 else 'sell',
                                        'timestamp': d.get('t', 0),
                                        'received_at': time.time()
                                    }
                                    self.trade_buffers[unified_sym].append(trade)
                                    
                                    # 🚀 DB Logging
                                    asyncio.create_task(self.logger.log_tick(
                                        self.exchange_id, unified_sym, trade['price'], trade['amount'], trade['side'], d.get('M', False)
                                    ))
                                except (ValueError, TypeError):
                                    continue
            except Exception as e:
                logging.error(f"Combined MEXC WS Error: {e}")
                await asyncio.sleep(5)

    async def scheduler_loop(self):
        while True:
            await asyncio.sleep(30)
            for symbol in self.symbols:
                try:
                    await self.process_chunk(symbol)
                except Exception as e:
                    logging.error(f"Error processing {symbol} on MEXC: {e}")

    async def process_chunk(self, symbol: str):
        trades = self.trade_buffers[symbol]
        self.trade_buffers[symbol] = []
        if not trades: return

        df = pd.DataFrame(trades)
        price_end = df['price'].iloc[-1]
        vol = df['amount'].sum()
        
        # === v5.1 Tick-DNA features ===
        df['ts_dt'] = pd.to_datetime(df['received_at'], unit='s')
        df['sec'] = df['ts_dt'].dt.second
        buy_df = df[df['side'] == 'buy']
        rush_counts = buy_df.groupby('sec').size().reindex(range(0, 60), fill_value=0)
        std_rush = float(rush_counts.std())

        costs = df['price'] * df['amount']
        avg_trade_size = float(costs.mean())
        max_trade_size = float(costs.max())
        median_trade_size = float(costs.median())
        buy_ratio = len(buy_df) / (len(df) + 1e-9)
        
        mid_ts = df['received_at'].min() + 15
        acceleration = (len(df[df['received_at'] >= mid_ts]) + 1) / (len(df[df['received_at'] < mid_ts]) + 1)
        
        prices = df['price']
        price_impact = (prices.max() - prices.iloc[0]) / (prices.iloc[0] + 1e-9) if len(prices) >= 2 else 0.0

        # Update history
        price_change = (price_end - df['price'].iloc[0]) / (df['price'].iloc[0] + 1e-9)
        new_row = {'volume': vol, 'price_change': price_change}
        self.history[symbol] = pd.concat([self.history[symbol], pd.DataFrame([new_row])]).iloc[-120:]

        if len(self.history[symbol]) < 10: return

        # Z-Scores
        hist = self.history[symbol]
        vol_z  = (vol - hist['volume'].mean()) / (hist['volume'].std() or 1)
        pc_z   = (price_change - hist['price_change'].mean()) / (hist['price_change'].std() or 1)

        features = {
            'symbol': symbol,
            'price': price_end,
            'vol_z': vol_z,
            'pc_z': pc_z,
            'pre_accum_z': 0.0, # Not enough history for deep pre-accum check yet
            'std_rush': std_rush,
            'avg_trade_size': avg_trade_size,
            'max_trade_size': max_trade_size,
            'median_trade_size': median_trade_size,
            'buy_ratio': buy_ratio,
            'acceleration': acceleration,
            'price_impact': price_impact,
            'market_cap': self.market_caps.get(symbol, 1_000_000),
            'oi_z': 0.0 # MEXC OI data is harder to fetch real-time in bulk
        }

        # Stage 3 ML Check
        if self.detector and self.detector.check_event(features)[0]:
            logging.warning(f"🚀 MEXC PUMP SIGNAL: {symbol} | vol_z={vol_z:.2f} pc_z={pc_z:.2f}")
            
            # Save the raw ticks for future training (The DNA of this pump)
            save_path = f"data/ticks_mexc_{symbol.replace('/','_').replace(':','_')}_{int(time.time())}.csv"
            os.makedirs("data", exist_ok=True)
            df.to_csv(save_path, index=False)
            logging.info(f"💾 MEXC Tick Data saved to {save_path}")

    async def run(self):
        await self.initialize()
        
        watchers = []
        # 🚀 Use Combined WebSocket
        watchers.append(asyncio.create_task(self.watch_combined_trades()))
            
        watchers.append(asyncio.create_task(self.scheduler_loop()))
        await asyncio.gather(*watchers)

if __name__ == "__main__":
    collector = MexcCollector()
    asyncio.run(collector.run())
