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

class UniverseManager:
    """
    Manages Stage 1 Data: Market Cap and Exchange Listings.
    Pumps mostly happen on coins < $60M Market Cap.
    """
    def __init__(self):
        self.market_caps: Dict[str, float] = {}
        self.exchange_counts: Dict[str, int] = {}
        self.last_update = 0

    async def update(self, exchange_id: str):
        """
        Periodically fetch market data for Stage 1 filtering.
        In production, this would use CoinGecko/CoinMarketCap API or Exchange Tickers.
        """
        now = time.time()
        if now - self.last_update < 3600:  # Update every hour
            return
            
        logging.info("Updating Token Universe (Stage 1 Data)...")
        # Placeholder: In a real bot, we'd fetch actual market caps here.
        # For now, we simulate or use default values for specific known symbols.
        self.market_caps = {'BTC/USDT': 1_000_000_000_000, 'ETH/USDT': 300_000_000_000}
        self.exchange_counts = {'BTC/USDT': 100, 'ETH/USDT': 100}
        self.last_update = now

    def get_data(self, symbol: str) -> Dict[str, float]:
        return {
            'market_cap': self.market_caps.get(symbol, 10_000_000),  # Default to low cap if unknown
            'exchanges': self.exchange_counts.get(symbol, 1)        # Default to low listings
        }

class MainCollector:
    """
    The main orchestrator for the High-Precision Pump Detection Bot.
    Combines Real-time CEX data, Feature Engineering, and Multi-Stage Detection.
    """
    def __init__(self, exchange_id: str = 'mexc', symbols: List[str] = None):
        self.exchange_id = exchange_id
        self.symbols = symbols or []
        self.exchange = getattr(ccxtpro, exchange_id)({
            'enableRateLimit': True,
        })
        self.db = DatabaseManager()
        self.detector = PumpDetector()
        self.universe = UniverseManager()
        
        self.trade_buffers: Dict[str, List[Dict]] = {s: [] for s in self.symbols}
        self.last_chunk_time: Dict[str, float] = {s: time.time() for s in self.symbols}
        self.history: Dict[str, pd.DataFrame] = {s: pd.DataFrame() for s in self.symbols}

    async def initialize(self):
        await self.db.connect()
        await self.universe.update(self.exchange_id)
        logging.info("MainCollector initialized and Stage 1 Universe data ready.")

    async def watch_trades(self, symbol: str):
        while True:
            try:
                trades = await self.exchange.watch_trades(symbol)
                for trade in trades:
                    trade['received_at'] = time.time()
                    self.trade_buffers[symbol].append(trade)
                
                now = time.time()
                if now - self.last_chunk_time[symbol] >= 30:
                    await self.process_chunk(symbol)
                    self.last_chunk_time[symbol] = now
                    
            except Exception as e:
                logging.error(f"Error in {symbol} trade loop: {e}")
                await asyncio.sleep(5)

    async def process_chunk(self, symbol: str):
        trades = self.trade_buffers[symbol]
        self.trade_buffers[symbol] = []
        if not trades: return

        df = pd.DataFrame(trades)
        
        # --- Feature Engineering ---
        total_vol = df['amount'].sum()
        price_end = df['price'].iloc[-1]
        price_change = (price_end - df['price'].iloc[0]) / df['price'].iloc[0]
        
        # StdRushOrders (1s buckets)
        df['sec'] = (df['received_at'] // 1).astype(int)
        rush_counts = df[df['side'] == 'buy'].groupby('sec').size().reindex(
            range(int(self.last_chunk_time[symbol]), int(self.last_chunk_time[symbol])+30), 
            fill_value=0
        )
        std_rush = rush_counts.std()
        
        # Update History & Calculate Z-Scores
        new_row = {'time': datetime.now(timezone.utc), 'volume': total_vol, 'price_change': price_change}
        self.history[symbol] = pd.concat([self.history[symbol], pd.DataFrame([new_row])]).iloc[-120:]
        
        if len(self.history[symbol]) < 20: return
        
        vol_z = (total_vol - self.history[symbol]['volume'].mean()) / self.history[symbol]['volume'].std()
        price_z = (price_change - self.history[symbol]['price_change'].mean()) / self.history[symbol]['price_change'].std()
        
        # --- Run Detection Cascade ---
        universe_data = self.universe.get_data(symbol)
        features = {
            'symbol': symbol,
            'market_cap': universe_data['market_cap'],
            'exchanges': universe_data['exchanges'],
            'vol_z': vol_z,
            'price_z': price_z,
            'std_rush_orders': std_rush,
            'taker_ratio': df[df['side'] == 'buy']['amount'].sum() / (df[df['side'] == 'sell']['amount'].sum() or 1),
            'volatility': self.history[symbol]['price_change'].std()
        }
        
        if self.detector.check_event(features):
            # 90% precision alert!
            print(f"!!! ALERT: {symbol} PUMP DETECTED !!!")

    async def run(self):
        await self.initialize()
        await asyncio.gather(*[self.watch_trades(s) for s in self.symbols])

if __name__ == "__main__":
    # Standard entry point
    # Usage: python collector.py
    # symbols = ['BTC/USDT', 'COIN_X/USDT', ...]
    collector = MainCollector(exchange_id='mexc', symbols=['BTC/USDT'])
    asyncio.run(collector.run())
