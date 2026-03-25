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
from database.data_logger import DataLogger

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MexcCollector:
    """
    Collects real-time MEXC trade data for Low-Cap/Diverse Pump detection.
    Ported from FuturesCollector (Binance) to handle MEXC-specific nuances.
    """
    def __init__(self, symbols: List[str] = None, db_manager: DatabaseManager = None):
        self.exchange_id = 'mexc'
        self.symbols = symbols or []
        self.exchange = ccxtpro.mexc({
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'}
        })
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
                                # Map BTC_USDT -> BTC/USDT:USDT
                                base = raw_sym.replace('_USDT', '')
                                ccxt_sym = f"{base}/USDT:USDT"
                                candidates.append(ccxt_sym)
                        
                        self.symbols = candidates[:30]
                        logging.info(f"Native MEXC Discovery Success: {len(self.symbols)} symbols.")
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
                    if m.get('active') and sym.endswith('/USDT:USDT'):
                        qv = float(m.get('info', {}).get('quoteVolume', 0)) or 0
                        if 100_000 < qv:
                             candidates.append((sym, qv))
                             self.market_caps[sym] = qv
                
                candidates.sort(key=lambda x: x[1], reverse=True)
                self.symbols = [s for s, _ in candidates[:30]]
                
                # Standard core targets
                u_targets = ["SIREN/USDT:USDT", "TRIA/USDT:USDT", "JCT/USDT:USDT", "LYN/USDT:USDT", "LIGHT/USDT:USDT"]
                for ut in u_targets:
                    if ut not in self.symbols: self.symbols.append(ut)
                
                logging.info(f"MEXC Discovery Success: {len(self.symbols)} symbols.")
            except Exception as e:
                logging.error(f"MEXC Discovery FAILED: {e}")
                self.symbols = ["SIREN/USDT:USDT", "TRIA/USDT:USDT", "JCT/USDT:USDT", "LYN/USDT:USDT"]
        
        for s in self.symbols:
            self.trade_buffers[s] = []
            self.last_chunk_time[s] = time.time()
            self.history[s] = pd.DataFrame()
            if s not in self.market_caps: self.market_caps[s] = 1_000_000
            
        logging.info(f"MexcCollector initialized ({len(self.symbols)} symbols).")

    async def watch_trades(self, symbol: str):
        logging.info(f"Starting MEXC trade watcher for {symbol}")
        while True:
            try:
                trades = await self.exchange.watch_trades(symbol)
                for trade in trades:
                    trade['received_at'] = time.time()
                    self.trade_buffers[symbol].append(trade)
                    # 🚀 Real-time tick logging to DB
                    asyncio.create_task(self.logger.log_tick(
                        self.exchange_id, 
                        symbol, 
                        trade['price'], 
                        trade['amount'], 
                        trade['side'], 
                        trade.get('info', {}).get('m', False)
                    ))
            except Exception as e:
                logging.error(f"Error in MEXC {symbol} loop: {e}")
                await asyncio.sleep(10)

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
        price_change = (price_end - df['price'].iloc[0]) / df['price'].iloc[0]
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
        for s in self.symbols:
            watchers.append(asyncio.create_task(self.watch_trades(s)))
            await asyncio.sleep(1.0)
            
        watchers.append(asyncio.create_task(self.scheduler_loop()))
        await asyncio.gather(*watchers)

if __name__ == "__main__":
    collector = MexcCollector()
    asyncio.run(collector.run())
